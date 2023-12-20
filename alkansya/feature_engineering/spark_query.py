from typing import Optional
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql import Column
from pyspark.sql.types import ArrayType, IntegerType
from alkansya.contants import OPEN, HIGH, LOW, CLOSE, VOLUME, TIME, CURRENCY_PAIR, SMA


def ohlcv(resolution_in_minutes: int) -> list[Column]:
    """Returns a list of pyspark queries for generating OHLCV summary statistics
     using the specified timeseries resolution.

    Parameters:
    -----------
        resolution_in_minutes: int
            The resolution to use for aggregating the OHLCV data.
    Returns:
    --------
        List of pyspark columns.
    """

    if resolution_in_minutes < 15:
        raise ValueError(
            """Resolution may not be less than 15 minutes due to data quality issues
            with the source."""
        )

    resolution_in_seconds = 60 * resolution_in_minutes

    w = (
        Window.partitionBy(CURRENCY_PAIR)
        .orderBy(f.col(TIME).cast("long"))
        .rangeBetween(-resolution_in_seconds, Window.currentRow)
    )

    queries = [
        f.last(f.col(TIME)).over(w).alias(TIME),
        f.first(f.col(OPEN)).over(w).alias(OPEN),
        f.max(f.col(HIGH)).over(w).alias(HIGH),
        f.min(f.col(LOW)).over(w).alias(LOW),
        f.last(f.col(CLOSE)).over(w).alias(CLOSE),
        f.sum(f.col(VOLUME)).over(w).alias(VOLUME),
        f.last(f.col(CURRENCY_PAIR)).over(w).alias(CURRENCY_PAIR),
    ]

    return queries


def sma(window_size_days: int):
    """ """
    lookback_window_size = 86400 * window_size_days

    w = (
        Window.partitionBy(CURRENCY_PAIR)
        .orderBy(f.col(TIME).cast("long"))
        .rangeBetween(-lookback_window_size, Window.currentRow)
    )

    return f.mean(f.col(CLOSE)).over(w).alias(f"{SMA}_{window_size_days}_day")


def ema(dfs, window_size_days: int):
    """ """
    lookback_window_size = 86400 * window_size_days
    generate_exponents = f.udf(
        lambda array_len: list(range(array_len - 1, -1, -1)),
        returnType=ArrayType(IntegerType()),
    )

    w = (
        Window.partitionBy(CURRENCY_PAIR)
        .orderBy(f.col(TIME).cast("long"))
        .rangeBetween(-lookback_window_size, Window.currentRow)
    )

    x_i = f.collect_list(f.col(CLOSE)).over(w).alias(f"{CLOSE}_collection")
    j_i = generate_exponents(f.size(x_i)).alias("position")
    alpha = 2 / ((f.size(x_i)) + 1)
    weight_temp = (1 - alpha) / (1 - f.pow(alpha, f.size(x_i)))
    repeat_alpha = f.array_repeat(alpha, f.size(x_i)).alias("alpha")
    repeat_weight_temp = f.array_repeat(weight_temp, f.size(x_i)).alias("weight_temp")
    ewma_factors = f.arrays_zip(x_i, j_i, repeat_alpha, repeat_weight_temp).alias(
        "ewma_factors"
    )
    ewma_addends = f.expr(
        f"transform(ewma_factors, x -> POWER(x['alpha'],x['position'])*x['{CLOSE}_collection']*x['weight_temp'])"
    ).alias("ewma_addends")

    ewma = f.expr(
        str="AGGREGATE(ewma_addends, cast(0 as double), (acc, x) -> acc + x)"
    ).alias("ewma")

    return (
        dfs.select("time", "currency_pair", ewma_factors)
        .select("*", ewma_addends)
        .select("*", ewma)
        .drop("ewma_factors", "ewma_addends")
    )


import os
import findspark
from pyspark.sql import SparkSession
from alkansya.utils import get_configurations

findspark.init()

os.environ["ENV"] = "DEV"

cfg = get_configurations()

path_to_silver = cfg["path_to_silver"]
path_to_gold = cfg["path_to_gold"]

spark = SparkSession.builder.master("local[*]").getOrCreate()

dfs_temp = ema(
    dfs=spark.read.parquet(f"{path_to_gold}/resolution_60min"), window_size_days=1
)

print(dfs_temp.dtypes)
dfs_temp.show()
