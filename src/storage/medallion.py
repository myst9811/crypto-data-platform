"""Medallion architecture implementation for Bronze/Silver/Gold layers."""

import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, current_timestamp, lit,
    to_timestamp, from_unixtime
)

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from processing.schemas import (
    TRADE_SCHEMA, ORDERBOOK_SCHEMA, TICKER_SCHEMA
)

logger = logging.getLogger(__name__)


class BronzeLayer:
    """
    Bronze Layer: Raw data ingestion from Kafka.

    Responsibilities:
    - Read from Kafka topics
    - Parse JSON messages
    - Add processing timestamp
    - Write to Delta Lake with minimal transformation
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Bronze Layer.

        Args:
            spark: Active Spark session
            config: Configuration dictionary with Kafka settings
        """
        self.spark = spark
        self.config = config
        self.kafka_bootstrap_servers = config.get('kafka', {}).get('bootstrap_servers', 'localhost:9092')
        self.kafka_topics = config.get('kafka', {}).get('subscribe', 'raw-trades,raw-orderbook,raw-ticker')

        logger.info("Bronze Layer initialized")

    def read_from_kafka(self) -> DataFrame:
        """
        Read streaming data from Kafka topics.

        Returns:
            Streaming DataFrame from Kafka
        """
        logger.info(f"Reading from Kafka: {self.kafka_topics}")

        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.kafka_topics)
            .option("startingOffsets", self.config.get('kafka', {}).get('starting_offsets', 'latest'))
            .option("failOnDataLoss", self.config.get('kafka', {}).get('fail_on_data_loss', 'false'))
            .option("maxOffsetsPerTrigger", self.config.get('kafka', {}).get('max_offsets_per_trigger', 10000))
            .load()
        )

        logger.info("Kafka stream loaded")
        return df

    def parse_messages(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        Parse Kafka messages and separate by topic.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Dictionary with topic names as keys and parsed DataFrames as values
        """
        logger.info("Parsing Kafka messages")

        # Convert value from binary to string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp")

        # Parse trades
        trades_df = (
            df.filter(col("topic") == "raw-trades")
            .select(
                from_json(col("value"), TRADE_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select("data.*", "kafka_timestamp")
            .withColumn("processing_timestamp", current_timestamp())
        )

        # Parse orderbook
        orderbook_df = (
            df.filter(col("topic") == "raw-orderbook")
            .select(
                from_json(col("value"), ORDERBOOK_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select("data.*", "kafka_timestamp")
            .withColumn("processing_timestamp", current_timestamp())
        )

        # Parse ticker
        ticker_df = (
            df.filter(col("topic") == "raw-ticker")
            .select(
                from_json(col("value"), TICKER_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select("data.*", "kafka_timestamp")
            .withColumn("processing_timestamp", current_timestamp())
        )

        return {
            "trades": trades_df,
            "orderbook": orderbook_df,
            "ticker": ticker_df
        }


class SilverLayer:
    """
    Silver Layer: Cleaned and normalized data.

    Responsibilities:
    - Read from Bronze Delta tables
    - Normalize symbols and formats
    - Perform data quality checks
    - Add derived fields
    - Write to Delta Lake Silver layer
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Silver Layer.

        Args:
            spark: Active Spark session
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.bronze_path = config.get('delta_lake', {}).get('bronze', {})

        logger.info("Silver Layer initialized")

    def read_from_bronze(self, data_type: str) -> DataFrame:
        """
        Read streaming data from Bronze Delta table.

        Args:
            data_type: Data type (trades, orderbook, ticker)

        Returns:
            Streaming DataFrame from Bronze layer
        """
        path = self.bronze_path.get(data_type)
        if not path:
            raise ValueError(f"No bronze path configured for {data_type}")

        logger.info(f"Reading from Bronze layer: {path}")

        df = (
            self.spark
            .readStream
            .format("delta")
            .load(path)
        )

        return df

    def clean_and_normalize(self, df: DataFrame, data_type: str) -> DataFrame:
        """
        Clean and normalize data.

        Args:
            df: Input DataFrame
            data_type: Data type (trades, orderbook, ticker)

        Returns:
            Cleaned and normalized DataFrame
        """
        logger.info(f"Cleaning and normalizing {data_type}")

        # Convert Unix timestamps to proper timestamps
        if "timestamp" in df.columns:
            df = df.withColumn(
                "timestamp",
                to_timestamp(from_unixtime(col("timestamp") / 1000))
            )

        # Add processing timestamp
        df = df.withColumn("silver_processing_timestamp", current_timestamp())

        # Remove nulls from critical fields
        if data_type == "trades":
            df = df.filter(
                col("price").isNotNull() &
                col("volume").isNotNull() &
                col("symbol").isNotNull()
            )

        elif data_type == "orderbook":
            df = df.filter(
                col("symbol").isNotNull() &
                (col("bids").isNotNull() | col("asks").isNotNull())
            )

        elif data_type == "ticker":
            df = df.filter(
                col("symbol").isNotNull() &
                col("last_price").isNotNull()
            )

        return df


class GoldLayer:
    """
    Gold Layer: Analytics and business logic.

    Responsibilities:
    - Read from Silver Delta tables
    - Apply business logic transformations
    - Perform aggregations
    - Detect arbitrage opportunities
    - Write to Delta Lake Gold layer
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Gold Layer.

        Args:
            spark: Active Spark session
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.silver_path = config.get('delta_lake', {}).get('silver', {})
        self.arbitrage_config = config.get('arbitrage', {})

        logger.info("Gold Layer initialized")

    def read_from_silver(self, data_type: str) -> DataFrame:
        """
        Read streaming data from Silver Delta table.

        Args:
            data_type: Data type (trades, orderbook, ticker, normalized_prices)

        Returns:
            Streaming DataFrame from Silver layer
        """
        path = self.silver_path.get(data_type)
        if not path:
            raise ValueError(f"No silver path configured for {data_type}")

        logger.info(f"Reading from Silver layer: {path}")

        df = (
            self.spark
            .readStream
            .format("delta")
            .load(path)
        )

        return df

    def apply_watermark(self, df: DataFrame, timestamp_col: str, delay: str = "30 seconds") -> DataFrame:
        """
        Apply watermark for handling late data.

        Args:
            df: Input DataFrame
            timestamp_col: Timestamp column name
            delay: Watermark delay

        Returns:
            DataFrame with watermark applied
        """
        return df.withWatermark(timestamp_col, delay)
