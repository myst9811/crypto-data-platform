"""Main Spark Structured Streaming application for crypto data pipeline."""

import logging
import signal
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.transformations import (
    normalize_symbol, normalize_prices, add_data_quality_score,
    detect_arbitrage_opportunities,
    calculate_vwap, aggregate_volume, calculate_liquidity_metrics
)
from processing.transformations.normalizer import extract_currency_pair
from storage.medallion import BronzeLayer, SilverLayer, GoldLayer
from storage.delta_writer import DeltaWriter

logger = logging.getLogger(__name__)


class CryptoStreamingApp:
    """
    Main orchestrator for the crypto data streaming pipeline.

    Coordinates Bronze -> Silver -> Gold medallion architecture with:
    - Kafka ingestion (Bronze)
    - Normalization and quality checks (Silver)
    - Aggregations and analytics (Gold)
    """

    def __init__(self, config_path: str = "config/spark_config.yaml"):
        """
        Initialize the streaming application.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config = self._load_config(config_path)
        self.spark: Optional[SparkSession] = None
        self.queries: List[StreamingQuery] = []
        self.is_running = False

        # Layer components (initialized in start())
        self.bronze_layer: Optional[BronzeLayer] = None
        self.silver_layer: Optional[SilverLayer] = None
        self.gold_layer: Optional[GoldLayer] = None
        self.delta_writer: Optional[DeltaWriter] = None

        logger.info("CryptoStreamingApp initialized")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        logger.info(f"Configuration loaded from {config_path}")
        return config

    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure SparkSession with Delta Lake support.

        Returns:
            Configured SparkSession
        """
        spark_config = self.config.get('spark', {})

        builder = (
            SparkSession.builder
            .appName(spark_config.get('app_name', 'crypto-streaming-pipeline'))
        )

        # Set master if configured (otherwise use local[*] as default)
        master = spark_config.get('master')
        if master:
            builder = builder.master(master)
        else:
            builder = builder.master("local[*]")

        # Apply Spark configurations
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, str(value))

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"SparkSession created: {spark.sparkContext.appName}")
        return spark

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers for SIGINT and SIGTERM."""
        def shutdown_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        logger.info("Signal handlers configured")

    def _initialize_layers(self):
        """Initialize medallion architecture layers."""
        self.bronze_layer = BronzeLayer(self.spark, self.config)
        self.silver_layer = SilverLayer(self.spark, self.config)
        self.gold_layer = GoldLayer(self.spark, self.config)
        self.delta_writer = DeltaWriter(
            base_path=self.config.get('delta_lake', {}).get('base_path', './data')
        )
        logger.info("Medallion layers initialized")

    # ============================================================
    # Bronze Layer Methods
    # ============================================================

    def _start_bronze_streams(self) -> List[StreamingQuery]:
        """
        Start Bronze layer streams (Kafka ingestion).

        Returns:
            List of StreamingQuery objects for Bronze layer
        """
        logger.info("Starting Bronze layer streams...")
        queries = []

        # Read from Kafka
        kafka_df = self.bronze_layer.read_from_kafka()

        # Parse messages by topic
        parsed_dfs = self.bronze_layer.parse_messages(kafka_df)

        # Get checkpoint and trigger config
        checkpoint_base = self.config.get('delta_lake', {}).get('checkpoints', {}).get('bronze', './data/checkpoints/bronze')
        trigger_interval = self.config.get('streaming', {}).get('trigger', {}).get('processing_time', '10 seconds')

        # Write each parsed stream to Bronze Delta tables
        for topic_name, df in parsed_dfs.items():
            checkpoint_path = f"{checkpoint_base}/{topic_name}"

            query = self.delta_writer.write_to_bronze(
                df=df,
                topic_name=topic_name,
                checkpoint_location=checkpoint_path,
                trigger_interval=trigger_interval
            )
            queries.append(query)
            logger.info(f"Bronze stream started: {topic_name}")

        return queries

    # ============================================================
    # Silver Layer Methods
    # ============================================================

    def _start_silver_streams(self) -> List[StreamingQuery]:
        """
        Start Silver layer streams (normalization and quality).

        Returns:
            List of StreamingQuery objects for Silver layer
        """
        logger.info("Starting Silver layer streams...")
        queries = []

        checkpoint_base = self.config.get('delta_lake', {}).get('checkpoints', {}).get('silver', './data/checkpoints/silver')
        trigger_interval = self.config.get('streaming', {}).get('trigger', {}).get('processing_time', '10 seconds')

        # Process trades
        trades_query = self._process_silver_trades(checkpoint_base, trigger_interval)
        if trades_query:
            queries.append(trades_query)

        # Process orderbook
        orderbook_query = self._process_silver_orderbook(checkpoint_base, trigger_interval)
        if orderbook_query:
            queries.append(orderbook_query)

        # Process ticker
        ticker_query = self._process_silver_ticker(checkpoint_base, trigger_interval)
        if ticker_query:
            queries.append(ticker_query)

        return queries

    def _process_silver_trades(self, checkpoint_base: str, trigger_interval: str) -> Optional[StreamingQuery]:
        """Process trades through Silver layer transformations."""
        try:
            # Read from Bronze
            df = self.silver_layer.read_from_bronze("trades")

            # Clean and normalize
            df = self.silver_layer.clean_and_normalize(df, "trades")

            # Apply transformations
            df = normalize_symbol(df)
            df = normalize_prices(df)
            df = add_data_quality_score(df)
            df = extract_currency_pair(df)

            # Write to Silver
            query = self.delta_writer.write_to_silver(
                df=df,
                data_type="normalized_prices",
                checkpoint_location=f"{checkpoint_base}/normalized_prices",
                partition_by=["standard_symbol"],
                trigger_interval=trigger_interval
            )

            logger.info("Silver trades stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Silver trades stream: {e}")
            return None

    def _process_silver_orderbook(self, checkpoint_base: str, trigger_interval: str) -> Optional[StreamingQuery]:
        """Process orderbook through Silver layer transformations."""
        try:
            df = self.silver_layer.read_from_bronze("orderbook")
            df = self.silver_layer.clean_and_normalize(df, "orderbook")
            df = normalize_symbol(df)

            query = self.delta_writer.write_to_silver(
                df=df,
                data_type="orderbook",
                checkpoint_location=f"{checkpoint_base}/orderbook",
                partition_by=["standard_symbol"],
                trigger_interval=trigger_interval
            )

            logger.info("Silver orderbook stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Silver orderbook stream: {e}")
            return None

    def _process_silver_ticker(self, checkpoint_base: str, trigger_interval: str) -> Optional[StreamingQuery]:
        """Process ticker through Silver layer transformations."""
        try:
            df = self.silver_layer.read_from_bronze("ticker")
            df = self.silver_layer.clean_and_normalize(df, "ticker")
            df = normalize_symbol(df)

            query = self.delta_writer.write_to_silver(
                df=df,
                data_type="ticker",
                checkpoint_location=f"{checkpoint_base}/ticker",
                partition_by=["standard_symbol"],
                trigger_interval=trigger_interval
            )

            logger.info("Silver ticker stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Silver ticker stream: {e}")
            return None

    # ============================================================
    # Gold Layer Methods
    # ============================================================

    def _start_gold_streams(self) -> List[StreamingQuery]:
        """
        Start Gold layer streams (aggregations and analytics).

        Returns:
            List of StreamingQuery objects for Gold layer
        """
        logger.info("Starting Gold layer streams...")
        queries = []

        checkpoint_base = self.config.get('delta_lake', {}).get('checkpoints', {}).get('gold', './data/checkpoints/gold')
        trigger_interval = self.config.get('streaming', {}).get('trigger', {}).get('processing_time', '10 seconds')
        watermark_delay = self.config.get('streaming', {}).get('watermark', {}).get('delay', '30 seconds')

        # Get window configurations
        windows_config = self.config.get('windows', {})

        # VWAP calculations
        vwap_query = self._process_gold_vwap(checkpoint_base, trigger_interval, watermark_delay, windows_config)
        if vwap_query:
            queries.append(vwap_query)

        # Volume aggregations
        volume_query = self._process_gold_volume(checkpoint_base, trigger_interval, watermark_delay, windows_config)
        if volume_query:
            queries.append(volume_query)

        # Liquidity metrics
        liquidity_query = self._process_gold_liquidity(checkpoint_base, trigger_interval, watermark_delay)
        if liquidity_query:
            queries.append(liquidity_query)

        # Arbitrage detection
        arbitrage_query = self._process_gold_arbitrage(checkpoint_base, trigger_interval, watermark_delay)
        if arbitrage_query:
            queries.append(arbitrage_query)

        return queries

    def _process_gold_vwap(
        self,
        checkpoint_base: str,
        trigger_interval: str,
        watermark_delay: str,
        windows_config: Dict
    ) -> Optional[StreamingQuery]:
        """Calculate VWAP metrics and write to Gold layer."""
        try:
            # Read normalized prices from Silver
            df = self.gold_layer.read_from_silver("normalized_prices")

            # Apply watermark
            df = self.gold_layer.apply_watermark(df, "timestamp", watermark_delay)

            # Calculate VWAP for primary window (1 minute)
            primary_window = windows_config.get('minute_1', {})
            vwap_df = calculate_vwap(
                df=df,
                window_duration=primary_window.get('duration', '1 minute'),
                slide_duration=primary_window.get('slide'),
                watermark_delay=watermark_delay
            )

            # Write to Gold layer
            query = self.delta_writer.write_to_gold(
                df=vwap_df,
                metric_type="vwap",
                checkpoint_location=f"{checkpoint_base}/vwap",
                output_mode="append",
                partition_by=["standard_symbol", "window_duration"],
                trigger_interval=trigger_interval
            )

            logger.info("Gold VWAP stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Gold VWAP stream: {e}")
            return None

    def _process_gold_volume(
        self,
        checkpoint_base: str,
        trigger_interval: str,
        watermark_delay: str,
        windows_config: Dict
    ) -> Optional[StreamingQuery]:
        """Calculate volume aggregations and write to Gold layer."""
        try:
            df = self.gold_layer.read_from_silver("normalized_prices")
            df = self.gold_layer.apply_watermark(df, "timestamp", watermark_delay)

            primary_window = windows_config.get('minute_1', {})
            vol_df = aggregate_volume(
                df=df,
                window_duration=primary_window.get('duration', '1 minute'),
                slide_duration=primary_window.get('slide'),
                watermark_delay=watermark_delay
            )

            query = self.delta_writer.write_to_gold(
                df=vol_df,
                metric_type="volume_aggregates",
                checkpoint_location=f"{checkpoint_base}/volume_aggregates",
                output_mode="append",
                partition_by=["standard_symbol"],
                trigger_interval=trigger_interval
            )

            logger.info("Gold volume aggregation stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Gold volume stream: {e}")
            return None

    def _process_gold_liquidity(
        self,
        checkpoint_base: str,
        trigger_interval: str,
        watermark_delay: str
    ) -> Optional[StreamingQuery]:
        """Calculate liquidity metrics from orderbook data."""
        try:
            df = self.gold_layer.read_from_silver("orderbook")
            df = self.gold_layer.apply_watermark(df, "timestamp", watermark_delay)

            liquidity_df = calculate_liquidity_metrics(
                df=df,
                depth_levels=10,
                watermark_delay=watermark_delay
            )

            query = self.delta_writer.write_to_gold(
                df=liquidity_df,
                metric_type="liquidity_metrics",
                checkpoint_location=f"{checkpoint_base}/liquidity_metrics",
                output_mode="append",
                partition_by=["standard_symbol", "exchange"],
                trigger_interval=trigger_interval
            )

            logger.info("Gold liquidity metrics stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Gold liquidity stream: {e}")
            return None

    def _process_gold_arbitrage(
        self,
        checkpoint_base: str,
        trigger_interval: str,
        watermark_delay: str
    ) -> Optional[StreamingQuery]:
        """Detect arbitrage opportunities across exchanges."""
        try:
            df = self.gold_layer.read_from_silver("normalized_prices")
            df = self.gold_layer.apply_watermark(df, "timestamp", watermark_delay)

            # Get arbitrage config
            arb_config = self.config.get('arbitrage', {})

            arb_df = detect_arbitrage_opportunities(
                df=df,
                threshold_percent=arb_config.get('threshold_percent', 0.5),
                min_volume=arb_config.get('min_volume', 1.0),
                window_duration=f"{arb_config.get('max_spread_age_seconds', 10)} seconds"
            )

            query = self.delta_writer.write_to_gold(
                df=arb_df,
                metric_type="arbitrage_opportunities",
                checkpoint_location=f"{checkpoint_base}/arbitrage_opportunities",
                output_mode="append",
                trigger_interval=trigger_interval
            )

            logger.info("Gold arbitrage detection stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to start Gold arbitrage stream: {e}")
            return None

    # ============================================================
    # Main Orchestration Methods
    # ============================================================

    def start(self):
        """
        Start the complete streaming pipeline.

        Initializes SparkSession, sets up layers, and starts all streams.
        """
        logger.info("Starting crypto streaming pipeline...")

        try:
            # Create Spark session
            self.spark = self._create_spark_session()

            # Setup signal handlers
            self._setup_signal_handlers()

            # Initialize medallion layers
            self._initialize_layers()

            self.is_running = True

            # Start Bronze layer (Kafka ingestion)
            bronze_queries = self._start_bronze_streams()
            self.queries.extend(bronze_queries)

            # Start Silver layer (normalization)
            silver_queries = self._start_silver_streams()
            self.queries.extend(silver_queries)

            # Start Gold layer (analytics)
            gold_queries = self._start_gold_streams()
            self.queries.extend(gold_queries)

            logger.info(f"Pipeline started with {len(self.queries)} streaming queries")

            # Await termination of all queries
            self._await_termination()

        except Exception as e:
            logger.error(f"Pipeline failed to start: {e}")
            self.stop()
            raise

    def _await_termination(self):
        """Wait for all streaming queries to terminate."""
        logger.info("Awaiting query termination...")

        try:
            # Use Spark's streaming query manager to await any termination
            self.spark.streams.awaitAnyTermination()

        except Exception as e:
            logger.error(f"Error during await termination: {e}")
            raise

    def stop(self):
        """
        Stop all streaming queries gracefully.

        Ensures proper cleanup of resources and checkpointing.
        """
        logger.info("Stopping streaming pipeline...")
        self.is_running = False

        # Stop all active queries
        for i, query in enumerate(self.queries):
            try:
                if query and query.isActive:
                    logger.info(f"Stopping query {i + 1}/{len(self.queries)}: {query.name or 'unnamed'}")
                    query.stop()
                    # Allow time for final checkpoint
                    query.awaitTermination(timeout=30)
            except Exception as e:
                logger.error(f"Error stopping query {i + 1}: {e}")

        self.queries.clear()

        # Stop Spark session
        if self.spark:
            logger.info("Stopping SparkSession...")
            self.spark.stop()
            self.spark = None

        logger.info("Streaming pipeline stopped")

    def get_query_status(self) -> List[Dict[str, Any]]:
        """
        Get status of all streaming queries.

        Returns:
            List of dictionaries with query status information
        """
        statuses = []
        for query in self.queries:
            if query:
                statuses.append({
                    'name': query.name,
                    'id': str(query.id),
                    'is_active': query.isActive,
                    'recent_progress': query.recentProgress,
                    'status': query.status
                })
        return statuses


def main():
    """Main entry point for the streaming application."""
    import argparse

    # Add path for utils import
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.logging_config import setup_logging

    parser = argparse.ArgumentParser(description='Crypto Data Streaming Pipeline')
    parser.add_argument(
        '--config',
        type=str,
        default='config/spark_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)

    # Create and start application
    app = CryptoStreamingApp(config_path=args.config)

    try:
        app.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        app.stop()


if __name__ == "__main__":
    main()
