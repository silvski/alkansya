# Run this once to clean all extracted files.

import os

import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from alkansya.contants import (
    CLOSE,
    CURRENCY_PAIR,
    HIGH,
    LOW,
    OPEN,
    TIME,
    VOLUME,
)
from alkansya.utils import get_configurations

findspark.init()

os.environ["ENV"] = "DEV"
cfg = get_configurations()

PATH_TO_BRONZE = cfg["path_to_bronze"]
PATH_TO_SILVER = cfg["path_to_silver"]
PATH_TO_GOLD = cfg["path_to_gold"]

spark = SparkSession.builder.master("local[*]").getOrCreate()

dfs = (
    spark.read.option("header", "true")
    .csv(f"{PATH_TO_SILVER}/*")
    .select(
        f.to_timestamp("Time", "yyyy.MM.dd HH:mm").alias(TIME),
        f.col("Open").cast("Float").alias(OPEN),
        f.col("High").cast("Float").alias(HIGH),
        f.col("Low").cast("Float").alias(LOW),
        f.col("Close").cast("Float").alias(CLOSE),
        f.col("Volume").cast("Float").alias(VOLUME),
        f.element_at(f.split(f.input_file_name(), "/"), -2).alias(CURRENCY_PAIR),
    )
)

dfs.write.partitionBy(CURRENCY_PAIR).parquet(
    f"{PATH_TO_GOLD}/cleaned", mode="overwrite"
)
