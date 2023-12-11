import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

findspark.init()

parent_directory = "C:/Users/Nigel/Documents/FOREX/SILVER"
target_directory = "C:/Users/Nigel/Documents/FOREX/GOLD"

spark = SparkSession.builder.master("local[*]").getOrCreate()

dfs = (
    spark.read.option("header", "true")
    .csv(f"{parent_directory}/*")
    .select(
        f.to_timestamp("Time", "yyyy.MM.dd HH:mm").alias("time"),
        f.col("Open").cast("Float").alias("open"),
        f.col("High").cast("Float").alias("high"),
        f.col("Low").cast("Float").alias("low"),
        f.col("Close").cast("Float").alias("close"),
        f.col("Volume").cast("Float").alias("volume"),
        f.element_at(f.split(f.input_file_name(), "/"), -2).alias("currency_pair"),
    )
)

dfs.write.partitionBy("currency_pair").parquet(
    f"{target_directory}/cleaned", mode="overwrite"
)
