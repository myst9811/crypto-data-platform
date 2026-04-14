"""Main Spark Structured Streaming application for crypto data pipeline.

Reads from Kafka, writes Bronze/Silver/Gold Delta Lake tables in local mode.
"""

import logging
import signal
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    from_json, col, current_timestamp, lit, when, window,
    to_timestamp, from_unixtime, udf,
    sum as spark_sum, count, avg,
    min as spark_min, max as spark_max, stddev,
    row_number, desc,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, BooleanType,
)
from pyspark.sql.window import Window as SparkWindow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

TRADE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("timestamp", LongType(), True),
    StructField("side", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("exchange", StringType(), False),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("raw_message", StringType(), True),
])

TICKER_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("symbol", StringType(), False),
    StructField("last_price", DoubleType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("price_change_percent", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("num_trades", LongType(), True),
    StructField("exchange", StringType(), False),
    StructField("best_bid", DoubleType(), True),
    StructField("best_ask", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("raw_message", StringType(), True),
])

# ---------------------------------------------------------------------------
# Symbol normalisation
# ---------------------------------------------------------------------------

SYMBOL_MAP = {
    "binance": {
        "BTCUSDT": "BTC/USD", "ETHUSDT": "ETH/USD",
        "BNBUSDT": "BNB/USD", "SOLUSDT": "SOL/USD", "XRPUSDT": "XRP/USD",
    },
    "coinbase": {
        "BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD",
        "SOL-USD": "SOL/USD", "XRP-USD": "XRP/USD",
    },
    "kraken": {
        "XBT/USD": "BTC/USD", "XBTUSD": "BTC/USD",
        "ETH/USD": "ETH/USD", "ETHUSD": "ETH/USD",
        "SOL/USD": "SOL/USD", "SOLUSD": "SOL/USD",
        "XRP/USD": "XRP/USD", "XRPUSD": "XRP/USD",
    },
}

ALL_SYMBOLS: Dict[str, str] = {}
for _exch_map in SYMBOL_MAP.values():
    ALL_SYMBOLS.update(_exch_map)


@udf(StringType())
def _normalise_symbol(exchange: str, symbol: str) -> Optional[str]:
    if not symbol or not exchange:
        return None
    mapping = SYMBOL_MAP.get(exchange.lower(), {})
    return mapping.get(symbol.upper(), mapping.get(symbol, symbol))


def normalise_symbol(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "standard_symbol", _normalise_symbol(col("exchange"), col("symbol"))
    )


# =========================================================================
# CryptoStreamingApp
# =========================================================================

class CryptoStreamingApp:
    """Orchestrates Bronze -> Silver -> Gold medallion pipeline in local mode."""

    JARS = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    ])

    def __init__(self, config_path: str = "config/spark_config.yaml"):
        self.config = self._load_config(config_path)
        self.spark: Optional[SparkSession] = None
        self.queries: List[StreamingQuery] = []
        self.is_running = False
        logger.info("CryptoStreamingApp initialised")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        p = Path(config_path)
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")
        with open(p) as f:
            return yaml.safe_load(f)

    def _create_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("crypto-streaming-pipeline")
            .master("local[2]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", self.JARS)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.databricks.delta.retentionDurationCheck.enabled",
                    "false")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created (local[2])")
        return spark

    def _setup_signal_handlers(self):
        def _handler(signum, frame):
            logger.info(f"Signal {signum} received — shutting down")
            self.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    @staticmethod
    def _ensure_dirs(*paths: str):
        for p in paths:
            Path(p).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Kafka reader
    # ------------------------------------------------------------------

    def _read_kafka(self, topics: str) -> DataFrame:
        kafka_cfg = self.config.get("kafka", {})
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",
                    kafka_cfg.get("bootstrap_servers", "localhost:9092"))
            .option("subscribe", topics)
            .option("startingOffsets",
                    kafka_cfg.get("starting_offsets", "latest"))
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger",
                    kafka_cfg.get("max_offsets_per_trigger", 10000))
            .load()
        )

    # ------------------------------------------------------------------
    # BRONZE LAYER
    # ------------------------------------------------------------------

    def _start_bronze(self) -> List[StreamingQuery]:
        logger.info("Starting Bronze layer streams")
        queries: List[StreamingQuery] = []

        raw = self._read_kafka("raw-trades,raw-ticker")
        raw = raw.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)",
            "topic", "timestamp AS kafka_timestamp",
        )

        # --- Bronze trades ---
        trades_raw = (
            raw.filter(col("topic") == "raw-trades")
            .withColumn("processing_timestamp", current_timestamp())
        )
        self._ensure_dirs("data/bronze/trades", "data/checkpoints/bronze/trades")
        q = (
            trades_raw.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "data/checkpoints/bronze/trades")
            .option("mergeSchema", "true")
            .trigger(processingTime="10 seconds")
            .start("data/bronze/trades")
        )
        queries.append(q)
        logger.info("Bronze trades stream started")

        # --- Bronze ticker ---
        ticker_raw = (
            raw.filter(col("topic") == "raw-ticker")
            .withColumn("processing_timestamp", current_timestamp())
        )
        self._ensure_dirs("data/bronze/ticker", "data/checkpoints/bronze/ticker")
        q = (
            ticker_raw.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "data/checkpoints/bronze/ticker")
            .option("mergeSchema", "true")
            .trigger(processingTime="10 seconds")
            .start("data/bronze/ticker")
        )
        queries.append(q)
        logger.info("Bronze ticker stream started")

        return queries

    # ------------------------------------------------------------------
    # SILVER LAYER
    # ------------------------------------------------------------------

    def _start_silver(self) -> List[StreamingQuery]:
        logger.info("Starting Silver layer streams")
        queries: List[StreamingQuery] = []

        raw = self._read_kafka("raw-trades,raw-ticker")
        raw = raw.selectExpr(
            "CAST(value AS STRING) AS json_value",
            "topic",
            "timestamp AS kafka_timestamp",
        )

        # --- Silver prices from trades ---
        trades = (
            raw.filter(col("topic") == "raw-trades")
            .select(
                from_json(col("json_value"), TRADE_SCHEMA).alias("d"),
                col("kafka_timestamp"),
            )
            .select("d.*", "kafka_timestamp")
            .filter(col("price").isNotNull() & col("volume").isNotNull())
        )
        trades = normalise_symbol(trades)
        trades = trades.withColumn(
            "event_time",
            when(
                col("timestamp").isNotNull(),
                to_timestamp(from_unixtime(col("timestamp") / 1000)),
            ).otherwise(col("kafka_timestamp")),
        )
        prices_from_trades = trades.select(
            col("standard_symbol").alias("symbol"),
            col("exchange"),
            col("price").cast(DoubleType()),
            col("volume").cast(DoubleType()),
            col("event_time"),
        ).filter(col("symbol").isNotNull())

        # --- Silver prices from ticker ---
        ticker = (
            raw.filter(col("topic") == "raw-ticker")
            .select(
                from_json(col("json_value"), TICKER_SCHEMA).alias("d"),
                col("kafka_timestamp"),
            )
            .select("d.*", "kafka_timestamp")
            .filter(col("last_price").isNotNull())
        )
        ticker = normalise_symbol(ticker)
        ticker = ticker.withColumn(
            "event_time",
            when(
                col("timestamp").isNotNull(),
                to_timestamp(from_unixtime(col("timestamp") / 1000)),
            ).otherwise(col("kafka_timestamp")),
        )
        prices_from_ticker = ticker.select(
            col("standard_symbol").alias("symbol"),
            col("exchange"),
            col("last_price").alias("price").cast(DoubleType()),
            col("volume").cast(DoubleType()),
            col("event_time"),
        ).filter(col("symbol").isNotNull())

        # Union both sources
        prices = prices_from_trades.union(prices_from_ticker)

        self._ensure_dirs("data/silver/prices", "data/checkpoints/silver/prices")
        q = (
            prices.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "data/checkpoints/silver/prices")
            .option("mergeSchema", "true")
            .trigger(processingTime="10 seconds")
            .start("data/silver/prices")
        )
        queries.append(q)
        logger.info("Silver prices stream started")

        return queries

    # ------------------------------------------------------------------
    # GOLD LAYER
    # ------------------------------------------------------------------

    def _start_gold(self) -> List[StreamingQuery]:
        logger.info("Starting Gold layer streams")
        queries: List[StreamingQuery] = []

        # Read silver prices as streaming source
        self._ensure_dirs("data/silver/prices")
        prices = (
            self.spark.readStream
            .format("delta")
            .load("data/silver/prices")
        )

        # 10-second watermark on event_time
        prices = prices.withWatermark("event_time", "10 seconds")

        # ---- Gold #1: VWAP (1-min tumbling window per symbol per exchange) ----
        vwap = (
            prices
            .withColumn("price_volume", col("price") * col("volume"))
            .groupBy(
                window(col("event_time"), "1 minute").alias("w"),
                col("symbol"),
                col("exchange"),
            )
            .agg(
                (spark_sum("price_volume") / spark_sum("volume")).alias("vwap"),
                spark_sum("volume").alias("total_volume"),
                spark_sum("price_volume").alias("total_value"),
                count("*").alias("num_trades"),
                spark_min("price").alias("min_price"),
                spark_max("price").alias("max_price"),
                avg("price").alias("avg_price"),
                stddev("price").alias("std_dev_price"),
            )
            .select(
                col("symbol"), col("exchange"), col("vwap"),
                col("total_volume"), col("total_value"), col("num_trades"),
                col("min_price"), col("max_price"), col("avg_price"),
                col("std_dev_price"),
                col("w.start").alias("window_start"),
                col("w.end").alias("window_end"),
                lit("1 minute").alias("window_duration"),
            )
        )

        self._ensure_dirs("data/gold/vwap", "data/checkpoints/gold/vwap")
        q = (
            vwap.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "data/checkpoints/gold/vwap")
            .option("mergeSchema", "true")
            .trigger(processingTime="10 seconds")
            .start("data/gold/vwap")
        )
        queries.append(q)
        logger.info("Gold VWAP stream started")

        # ---- Gold #2: Spreads (cross-exchange spread table) ----
        # Re-read silver to get a separate streaming source for spreads
        prices2 = (
            self.spark.readStream
            .format("delta")
            .load("data/silver/prices")
        ).withWatermark("event_time", "10 seconds")

        # Get min-price and max-price exchanges per symbol per 1-min window
        # using window function approach: self-join on windowed aggregates
        price_agg = (
            prices2.groupBy(
                window(col("event_time"), "1 minute").alias("w"),
                col("symbol"),
                col("exchange"),
            )
            .agg(
                avg("price").alias("avg_price"),
                spark_sum("volume").alias("total_volume"),
            )
        )

        # We need a cross-join of exchanges within each window+symbol
        # Spark structured streaming doesn't support self-joins on streams easily.
        # Instead, we use foreachBatch to compute spreads as a micro-batch operation.

        def _compute_spreads(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return

            from pyspark.sql.functions import col as c
            from itertools import combinations

            # Get distinct window+symbol+exchange combinations
            agg = (
                batch_df.groupBy("w", "symbol", "exchange")
                .agg(
                    avg("avg_price").alias("price"),
                    spark_sum("total_volume").alias("volume"),
                )
            )

            # Self-join for cross-exchange pairs
            a = agg.alias("a")
            b = agg.alias("b")
            spreads = (
                a.join(
                    b,
                    (col("a.w") == col("b.w"))
                    & (col("a.symbol") == col("b.symbol"))
                    & (col("a.exchange") < col("b.exchange")),
                )
                .select(
                    col("a.symbol").alias("symbol"),
                    col("a.exchange").alias("exchange_a"),
                    col("b.exchange").alias("exchange_b"),
                    col("a.price").alias("price_a"),
                    col("b.price").alias("price_b"),
                    (col("b.price") - col("a.price")).alias("spread_abs_raw"),
                    col("a.w.start").alias("window_start"),
                    col("a.w.end").alias("window_end"),
                )
            )

            from pyspark.sql.functions import abs as spark_abs

            spreads = spreads.withColumn(
                "spread_abs", spark_abs(col("spread_abs_raw"))
            )
            spreads = spreads.withColumn(
                "spread_pct",
                when(
                    spark_min(col("price_a"), col("price_b")) > 0,
                    col("spread_abs")
                    / spark_min(col("price_a"), col("price_b")),
                ).otherwise(lit(0.0)),
            )
            spreads = spreads.withColumn("event_time", col("window_end"))

            spreads = spreads.select(
                "symbol", "exchange_a", "exchange_b",
                "price_a", "price_b", "spread_abs", "spread_pct",
                "event_time", "window_start", "window_end",
            )

            if spreads.count() > 0:
                (
                    spreads.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save("data/gold/spreads")
                )

        self._ensure_dirs("data/gold/spreads", "data/checkpoints/gold/spreads")
        q = (
            price_agg.writeStream
            .foreachBatch(_compute_spreads)
            .option("checkpointLocation", "data/checkpoints/gold/spreads")
            .trigger(processingTime="10 seconds")
            .start()
        )
        queries.append(q)
        logger.info("Gold spreads stream started")

        # ---- Gold #3: Arbitrage signals (spread_pct > 0.15%) ----
        # Re-read silver again for a separate stream
        prices3 = (
            self.spark.readStream
            .format("delta")
            .load("data/silver/prices")
        ).withWatermark("event_time", "10 seconds")

        price_agg3 = (
            prices3.groupBy(
                window(col("event_time"), "1 minute").alias("w"),
                col("symbol"),
                col("exchange"),
            )
            .agg(
                avg("price").alias("avg_price"),
                spark_sum("volume").alias("total_volume"),
            )
        )

        def _compute_arbitrage_signals(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return

            agg = (
                batch_df.groupBy("w", "symbol", "exchange")
                .agg(
                    avg("avg_price").alias("price"),
                    spark_sum("total_volume").alias("volume"),
                )
            )

            a = agg.alias("a")
            b = agg.alias("b")
            spreads = (
                a.join(
                    b,
                    (col("a.w") == col("b.w"))
                    & (col("a.symbol") == col("b.symbol"))
                    & (col("a.exchange") < col("b.exchange")),
                )
                .select(
                    col("a.symbol").alias("symbol"),
                    col("a.exchange").alias("exchange_a"),
                    col("b.exchange").alias("exchange_b"),
                    col("a.price").alias("price_a"),
                    col("b.price").alias("price_b"),
                    col("a.w.start").alias("window_start"),
                    col("a.w.end").alias("window_end"),
                )
            )

            from pyspark.sql.functions import abs as spark_abs

            spreads = spreads.withColumn(
                "spread_abs",
                spark_abs(col("price_b") - col("price_a")),
            )
            spreads = spreads.withColumn(
                "spread_pct",
                when(
                    spark_min(col("price_a"), col("price_b")) > 0,
                    col("spread_abs")
                    / spark_min(col("price_a"), col("price_b")),
                ).otherwise(lit(0.0)),
            )
            spreads = spreads.withColumn("event_time", col("window_end"))

            # Filter: spread_pct > 0.0015 (0.15%)
            signals = spreads.filter(col("spread_pct") > 0.0015)
            signals = signals.withColumn(
                "signal_timestamp", current_timestamp()
            )
            signals = signals.select(
                "symbol", "exchange_a", "exchange_b",
                "price_a", "price_b", "spread_abs", "spread_pct",
                "event_time", "window_start", "window_end",
                "signal_timestamp",
            )

            if signals.count() > 0:
                (
                    signals.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save("data/gold/arbitrage_signals")
                )

        self._ensure_dirs(
            "data/gold/arbitrage_signals",
            "data/checkpoints/gold/arbitrage_signals",
        )
        q = (
            price_agg3.writeStream
            .foreachBatch(_compute_arbitrage_signals)
            .option("checkpointLocation",
                    "data/checkpoints/gold/arbitrage_signals")
            .trigger(processingTime="10 seconds")
            .start()
        )
        queries.append(q)
        logger.info("Gold arbitrage signals stream started")

        return queries

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def _wait_for_silver(self, timeout_seconds: int = 120):
        """Wait until the Silver prices Delta table has data."""
        import time as _time
        from pathlib import Path as _P
        from delta import DeltaTable as _DT

        logger.info("Waiting for Silver prices table to be ready...")
        deadline = _time.time() + timeout_seconds
        while _time.time() < deadline:
            try:
                if _P("data/silver/prices/_delta_log").exists():
                    _DT.forPath(self.spark, "data/silver/prices")
                    logger.info("Silver prices table is ready")
                    return
            except Exception:
                pass
            _time.sleep(5)

        # If timeout, seed an empty table so Gold can start streaming
        logger.warning("Silver table not ready in time — seeding empty schema")
        from pyspark.sql.types import (
            StructType as _ST, StructField as _SF,
            StringType as _Str, DoubleType as _Dbl, TimestampType as _Ts,
        )
        schema = _ST([
            _SF("symbol", _Str()), _SF("exchange", _Str()),
            _SF("price", _Dbl()), _SF("volume", _Dbl()),
            _SF("event_time", _Ts()),
        ])
        empty = self.spark.createDataFrame([], schema)
        empty.write.format("delta").mode("overwrite").save("data/silver/prices")
        logger.info("Seeded empty Silver prices table")

    def start(self):
        logger.info("Starting crypto streaming pipeline")
        self.spark = self._create_spark_session()
        self._setup_signal_handlers()
        self.is_running = True

        # Bronze + Silver start first (both read from Kafka)
        bronze_qs = self._start_bronze()
        self.queries.extend(bronze_qs)

        silver_qs = self._start_silver()
        self.queries.extend(silver_qs)

        # Wait for Silver to produce at least one batch before Gold starts
        self._wait_for_silver(timeout_seconds=120)

        gold_qs = self._start_gold()
        self.queries.extend(gold_qs)

        logger.info(f"Pipeline running with {len(self.queries)} streaming queries")
        self.spark.streams.awaitAnyTermination()

    def stop(self):
        logger.info("Stopping streaming pipeline")
        self.is_running = False
        for i, q in enumerate(self.queries):
            try:
                if q and q.isActive:
                    q.stop()
                    q.awaitTermination(timeout=30)
            except Exception as e:
                logger.error(f"Error stopping query {i}: {e}")
        self.queries.clear()
        if self.spark:
            self.spark.stop()
            self.spark = None
        logger.info("Pipeline stopped")

    def get_query_status(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": q.name,
                "id": str(q.id),
                "is_active": q.isActive,
                "recent_progress": q.recentProgress,
                "status": q.status,
            }
            for q in self.queries
            if q
        ]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Crypto Data Streaming Pipeline"
    )
    parser.add_argument(
        "--config", default="config/spark_config.yaml",
        help="Path to configuration YAML",
    )
    args = parser.parse_args()

    app = CryptoStreamingApp(config_path=args.config)
    try:
        app.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        app.stop()


if __name__ == "__main__":
    main()
