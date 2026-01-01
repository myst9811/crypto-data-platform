"""Delta Lake writer utilities for streaming data."""

import logging
from typing import Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pathlib import Path

logger = logging.getLogger(__name__)


class DeltaWriter:
    """Utility class for writing data to Delta Lake."""

    def __init__(self, base_path: str = "./data"):
        """
        Initialize Delta Writer.

        Args:
            base_path: Base path for Delta Lake tables
        """
        self.base_path = base_path
        self._ensure_paths_exist()

    def _ensure_paths_exist(self):
        """Create necessary directory paths if they don't exist."""
        paths = [
            f"{self.base_path}/bronze",
            f"{self.base_path}/silver",
            f"{self.base_path}/gold",
            f"{self.base_path}/checkpoints"
        ]

        for path in paths:
            Path(path).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured path exists: {path}")

    def write_to_bronze(
        self,
        df: DataFrame,
        topic_name: str,
        checkpoint_location: str,
        trigger_interval: str = "10 seconds"
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to Bronze layer.

        Args:
            df: Streaming DataFrame
            topic_name: Kafka topic name (trades, orderbook, ticker)
            checkpoint_location: Checkpoint directory path
            trigger_interval: Trigger processing time

        Returns:
            StreamingQuery object
        """
        output_path = f"{self.base_path}/bronze/{topic_name}"

        logger.info(f"Writing to Bronze layer: {output_path}")

        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(processingTime=trigger_interval)
            .start(output_path)
        )

        logger.info(f"Bronze layer stream started for {topic_name}")
        return query

    def write_to_silver(
        self,
        df: DataFrame,
        data_type: str,
        checkpoint_location: str,
        partition_by: Optional[List[str]] = None,
        trigger_interval: str = "10 seconds"
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to Silver layer.

        Args:
            df: Streaming DataFrame
            data_type: Data type (trades, orderbook, ticker, normalized_prices)
            checkpoint_location: Checkpoint directory path
            partition_by: Optional list of columns to partition by
            trigger_interval: Trigger processing time

        Returns:
            StreamingQuery object
        """
        output_path = f"{self.base_path}/silver/{data_type}"

        logger.info(f"Writing to Silver layer: {output_path}")

        writer = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(processingTime=trigger_interval)
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        query = writer.start(output_path)

        logger.info(f"Silver layer stream started for {data_type}")
        return query

    def write_to_gold(
        self,
        df: DataFrame,
        metric_type: str,
        checkpoint_location: str,
        output_mode: str = "append",
        partition_by: Optional[List[str]] = None,
        trigger_interval: str = "10 seconds"
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to Gold layer.

        Args:
            df: Streaming DataFrame
            metric_type: Metric type (arbitrage_opportunities, vwap, volume_aggregates, liquidity_metrics)
            checkpoint_location: Checkpoint directory path
            output_mode: Output mode (append, update, complete)
            partition_by: Optional list of columns to partition by
            trigger_interval: Trigger processing time

        Returns:
            StreamingQuery object
        """
        output_path = f"{self.base_path}/gold/{metric_type}"

        logger.info(f"Writing to Gold layer: {output_path}")

        writer = (
            df.writeStream
            .format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(processingTime=trigger_interval)
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        query = writer.start(output_path)

        logger.info(f"Gold layer stream started for {metric_type}")
        return query

    def write_batch_to_delta(
        self,
        df: DataFrame,
        path: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None
    ):
        """
        Write batch DataFrame to Delta Lake.

        Args:
            df: Batch DataFrame
            path: Output path
            mode: Write mode (append, overwrite, etc.)
            partition_by: Optional list of columns to partition by
        """
        logger.info(f"Writing batch data to: {path}")

        writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)

        logger.info(f"Batch data written to: {path}")

    @staticmethod
    def stop_query(query: StreamingQuery, query_name: str = ""):
        """
        Stop a streaming query gracefully.

        Args:
            query: StreamingQuery to stop
            query_name: Optional name for logging
        """
        if query and query.isActive:
            logger.info(f"Stopping query{': ' + query_name if query_name else ''}...")
            query.stop()
            query.awaitTermination(timeout=30)
            logger.info(f"Query stopped{': ' + query_name if query_name else ''}")
        else:
            logger.warning(f"Query not active{': ' + query_name if query_name else ''}")

    @staticmethod
    def await_termination(queries: List[StreamingQuery]):
        """
        Wait for all streaming queries to terminate.

        Args:
            queries: List of StreamingQuery objects
        """
        logger.info(f"Awaiting termination of {len(queries)} queries...")

        try:
            for query in queries:
                if query and query.isActive:
                    query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, stopping all queries...")
            for query in queries:
                if query and query.isActive:
                    query.stop()

        logger.info("All queries terminated")
