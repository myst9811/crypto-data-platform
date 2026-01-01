"""Delta Lake utility functions and managers."""

from typing import Optional, Dict, Any
from pathlib import Path
import logging
from pyspark.sql import SparkSession, DataFrame
from delta import DeltaTable

logger = logging.getLogger(__name__)


class DeltaLakeManager:
    """Manager for Delta Lake operations."""

    def __init__(self, spark: SparkSession):
        """
        Initialize Delta Lake manager.

        Args:
            spark: Active Spark session
        """
        self.spark = spark
        logger.info("Delta Lake manager initialized")

    def write_to_delta(
        self,
        df: DataFrame,
        path: str,
        mode: str = "append",
        partition_by: Optional[list] = None,
        merge_schema: bool = True,
        optimize: bool = False
    ) -> None:
        """
        Write DataFrame to Delta Lake.

        Args:
            df: DataFrame to write
            path: Delta table path
            mode: Write mode (append, overwrite, etc.)
            partition_by: Optional list of columns to partition by
            merge_schema: Whether to merge schema automatically
            optimize: Whether to optimize after writing
        """
        try:
            writer = df.write.format("delta").mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            if merge_schema:
                writer = writer.option("mergeSchema", "true")

            writer.save(path)
            logger.info(f"Successfully wrote data to Delta table: {path}")

            if optimize:
                self.optimize_table(path)

        except Exception as e:
            logger.error(f"Failed to write to Delta table {path}: {e}")
            raise

    def read_from_delta(
        self,
        path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Read DataFrame from Delta Lake.

        Args:
            path: Delta table path
            version: Optional version to read
            timestamp: Optional timestamp to read (time travel)

        Returns:
            DataFrame from Delta table
        """
        try:
            reader = self.spark.read.format("delta")

            if version is not None:
                reader = reader.option("versionAsOf", version)
            elif timestamp is not None:
                reader = reader.option("timestampAsOf", timestamp)

            df = reader.load(path)
            logger.info(f"Successfully read data from Delta table: {path}")
            return df

        except Exception as e:
            logger.error(f"Failed to read from Delta table {path}: {e}")
            raise

    def merge_data(
        self,
        target_path: str,
        source_df: DataFrame,
        merge_condition: str,
        update_set: Dict[str, str],
        insert_values: Dict[str, str]
    ) -> None:
        """
        Perform MERGE operation (upsert) on Delta table.

        Args:
            target_path: Target Delta table path
            source_df: Source DataFrame
            merge_condition: Merge condition (e.g., "target.id = source.id")
            update_set: Columns to update when matched
            insert_values: Columns to insert when not matched
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)

            merge_builder = delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            )

            if update_set:
                merge_builder = merge_builder.whenMatchedUpdate(set=update_set)

            if insert_values:
                merge_builder = merge_builder.whenNotMatchedInsert(values=insert_values)

            merge_builder.execute()
            logger.info(f"Successfully merged data into Delta table: {target_path}")

        except Exception as e:
            logger.error(f"Failed to merge data into Delta table {target_path}: {e}")
            raise

    def optimize_table(self, path: str, z_order_by: Optional[list] = None) -> None:
        """
        Optimize Delta table (compaction and optional Z-ordering).

        Args:
            path: Delta table path
            z_order_by: Optional list of columns for Z-ordering
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)

            optimize_cmd = delta_table.optimize()

            if z_order_by:
                optimize_cmd = optimize_cmd.executeZOrderBy(*z_order_by)
            else:
                optimize_cmd.executeCompaction()

            logger.info(f"Successfully optimized Delta table: {path}")

        except Exception as e:
            logger.error(f"Failed to optimize Delta table {path}: {e}")
            raise

    def vacuum_table(self, path: str, retention_hours: int = 168) -> None:
        """
        Vacuum Delta table to remove old files.

        Args:
            path: Delta table path
            retention_hours: Retention period in hours (default 7 days)
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            delta_table.vacuum(retention_hours)
            logger.info(
                f"Successfully vacuumed Delta table: {path} "
                f"with retention {retention_hours} hours"
            )

        except Exception as e:
            logger.error(f"Failed to vacuum Delta table {path}: {e}")
            raise

    def get_table_history(self, path: str, limit: int = 10) -> DataFrame:
        """
        Get Delta table history.

        Args:
            path: Delta table path
            limit: Number of history entries to return

        Returns:
            DataFrame with table history
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            history = delta_table.history(limit)
            return history

        except Exception as e:
            logger.error(f"Failed to get history for Delta table {path}: {e}")
            raise

    def table_exists(self, path: str) -> bool:
        """
        Check if Delta table exists.

        Args:
            path: Delta table path

        Returns:
            True if table exists, False otherwise
        """
        try:
            return DeltaTable.isDeltaTable(self.spark, path)
        except Exception:
            return False

    @staticmethod
    def create_path_if_not_exists(path: str) -> None:
        """
        Create directory path if it doesn't exist.

        Args:
            path: Directory path to create
        """
        Path(path).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured path exists: {path}")
