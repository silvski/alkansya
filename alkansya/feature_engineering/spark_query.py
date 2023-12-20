from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql import Column


def candle_sticks(resolution_in_minutes: int) -> list[Column]:
    """Returns a pyspark query for generating candle stick summary statistics
     using the specified resolution.

    Parameters:
    -----------
        resolution_in_minutes: int
            The resolution to use for aggregating the candlestick data.
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
        Window.partitionBy(f.col("currency_pair"))
        .orderBy(f.col("time").cast("long"))
        .rangeBetween(-resolution_in_seconds, Window.currentRow)
    )

    queries = [
        f.last(f.col("time")).over(w).alias("time"),
        f.first(f.col("open")).over(w).alias("open"),
        f.max(f.col("high")).over(w).alias("high"),
        f.min(f.col("low")).over(w).alias("low"),
        f.last(f.col("close")).over(w).alias("close"),
        f.sum(f.col("volume")).over(w).alias("volume"),
        f.last(f.col("currency_pair")).over(w).alias("currency_pair"),
    ]

    return queries
