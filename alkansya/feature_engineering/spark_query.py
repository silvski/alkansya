from scipy.signal import find_peaks
from typing import Optional
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql import Column
from pyspark.sql.types import ArrayType, IntegerType, DoubleType
from alkansya.contants import OPEN, HIGH, LOW, CLOSE, VOLUME, TIME, CURRENCY_PAIR, SMA
from alkansya.utils import convert_window_size_to_seconds


def ohlcv(resolution_in_minutes: int) -> list[Column]:
    """Returns a list of pyspark queries for generating OHLCV summary statistics for the
    given resolution.

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


def simple_moving_average(window_size_days: int) -> Column:
    """Returns a pyspark query for calculating the simple moving average.

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """

    lookback_window_size = convert_window_size_to_seconds(window_size_days, "days")

    w = (
        Window.partitionBy(CURRENCY_PAIR)
        .orderBy(f.col(TIME).cast("long"))
        .rangeBetween(-lookback_window_size, Window.currentRow)
    )

    return f.mean(f.col(CLOSE)).over(w).alias(f"{SMA}_{window_size_days}_day")


def exponential_moving_average(
    window_size_days: int, smoothing_factor: float = 2.0
) -> Column:
    """Returns a pyspark query for calculating the exponential moving average.

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """

    lookback_window_size = convert_window_size_to_seconds(window_size_days, "days")
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
    j_i = generate_exponents(f.size(x_i)).alias("power")
    alpha = smoothing_factor / ((f.size(x_i)) + 1)
    weight_temp = (1 - alpha) / (1 - f.pow(alpha, f.size(x_i)))
    repeat_alpha = f.array_repeat(alpha, f.size(x_i)).alias("alpha")
    zipped_alpha_x_j = f.arrays_zip(repeat_alpha, j_i, x_i)

    ema_addends = (
        f.transform(
            zipped_alpha_x_j,
            lambda x: f.pow(x["alpha"], x["power"]) * x[f"{CLOSE}_collection"],
        )
    ).alias("ema_addends")

    ema = (
        f.aggregate(ema_addends, f.lit(0.0), lambda acc, x: acc + x) * weight_temp
    ).alias(f"ema_{window_size_days}_day")

    return ema


def volatility(window_size_days: int) -> Column:
    """Returns a pyspark query for calculating the volatility.

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """

    lookback_window_size = convert_window_size_to_seconds(window_size_days, "days")

    w = (
        Window.partitionBy(CURRENCY_PAIR)
        .orderBy(f.col(TIME).cast("long"))
        .rangeBetween(-lookback_window_size, Window.currentRow)
    )

    return (f.stddev(CLOSE).over(w) / f.size(f.collect_list(CLOSE).over(w))).alias(
        f"volatility_{window_size_days}_day"
    )


def support_and_resistance(window_size_days: int) -> list[Column]:
    """Returns a list of pyspark queries for calculating the basic support and
    resistance.

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """

    lookback_window_size = convert_window_size_to_seconds(window_size_days, "days")
    get_peaks = f.udf(lambda x: find_peaks(x)[0], returnType=ArrayType(DoubleType()))

    w = (
        Window.partitionBy(CURRENCY_PAIR)
        .orderBy(f.col(TIME).cast("long"))
        .rangeBetween(-lookback_window_size, Window.currentRow)
    )

    upper_bounds = f.greatest(OPEN, CLOSE).alias("upper_bound")
    lower_bounds = f.least(OPEN, CLOSE).alias("lower_bound")

    peak_indeces = get_peaks(f.collect_list(upper_bounds).over(w)).alias("peak_indeces")
    dip_indeces = get_peaks(f.collect_list((-1 * lower_bounds)).over(w)).alias(
        "dip_indeces"
    )

    return [upper_bounds, lower_bounds, peak_indeces, dip_indeces]
