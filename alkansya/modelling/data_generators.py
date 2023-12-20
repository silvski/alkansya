from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from typing import Optional
from alkansya.feature_engineering import spark_query as query


def generate_candle_sticks(
    dfs: DataFrame,
    resolutions: list[int],
    target_path: str,
    partition_columns: Optional[list[str]] = None,
) -> None:
    """ """

    for resolution in resolutions:
        (
            dfs.select(*query.candle_sticks(resolution_in_minutes=resolution))
            .filter(f.col("time").cast("long") % (60 * resolution) == 0)
            .write.partitionBy(partition_columns)
            .parquet(f"{target_path}/resolution_{resolution}min", mode="overwrite")
        )
