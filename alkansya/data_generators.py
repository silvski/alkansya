from alkansya.feature_engineering import spark_query as query
from typing import Optional
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from alkansya.contants import TIME, CURRENCY_PAIR


def generate_ohlcv(
    dfs: DataFrame,
    resolution_in_minutes: int,
    target_path: str,
) -> None:
    """Calculate the ohlcv statistics and write it to the target directory.

    Parameters:
    -----------
        dfs: pyspark.sql.DataFrame
            The input data as a pyspark.sql.DataFrame
        resolution_in_minutes: int
            The resolution to use for aggregating the candlestick data.
        target_path: str
            The name of the target directory.
        partition_columns: list[str], default = None
            A list of column names for partitioning the window transformations.
    Returns:
    --------
        None
    """

    new_target_path = f"{target_path}/resolution_{resolution_in_minutes}min"

    (
        dfs.select(*query.ohlcv(resolution_in_minutes=resolution_in_minutes))
        .filter(f.col(TIME).cast("long") % (60 * resolution_in_minutes) == 0)
        .write.partitionBy(CURRENCY_PAIR)
        .parquet(new_target_path, mode="overwrite")
    )
