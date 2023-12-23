import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from alkansya.contants import OPEN, HIGH, LOW, CLOSE, VOLUME, TIME, CURRENCY_PAIR

findspark.init()

parent_directory = "C:/Users/Nigel/Documents/FOREX/SILVER"
target_directory = "C:/Users/Nigel/Documents/FOREX/GOLD"

spark = SparkSession.builder.master("local[*]").getOrCreate()

dfs = (
    spark.read.option("header", "true")
    .csv(f"{parent_directory}/*")
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
    f"{target_directory}/cleaned", mode="overwrite"
)
