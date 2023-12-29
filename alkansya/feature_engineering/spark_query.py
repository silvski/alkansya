from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, IntegerType
from scipy.signal import find_peaks

from alkansya.contants import CLOSE, CURRENCY_PAIR, HIGH, LOW, OPEN, SMA, TIME, VOLUME
from alkansya.spark_utils import WindowMaker


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

    w = WindowMaker().create_lookback_window(
        window_size=resolution_in_minutes,
        unit_of_time="minutes",
        order_col=f.col(TIME).cast("long"),
        partition_col=CURRENCY_PAIR,
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
    """Returns a pyspark query for calculating the CLOSE simple moving average.

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """

    w = WindowMaker().create_lookback_window(
        window_size=window_size_days,
        unit_of_time="days",
        order_col=f.col(TIME).cast("long"),
        partition_col=CURRENCY_PAIR,
    )

    return f.mean(f.col(CLOSE)).over(w).alias(f"{SMA}_{window_size_days}_day")


def exponential_moving_average(
    window_size_days: int, smoothing_factor: float = 2.0
) -> Column:
    """Returns a pyspark query for calculating the CLOSE exponential moving average.

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """
    w = WindowMaker().create_lookback_window(
        window_size=window_size_days,
        unit_of_time="days",
        order_col=f.col(TIME).cast("long"),
        partition_col=CURRENCY_PAIR,
    )

    generate_exponents = f.udf(
        lambda array_len: list(range(array_len - 1, -1, -1)),
        returnType=ArrayType(IntegerType()),
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

    w = WindowMaker().create_lookback_window(
        window_size=window_size_days,
        unit_of_time="days",
        order_col=f.col(TIME).cast("long"),
        partition_col=CURRENCY_PAIR,
    )

    return (f.stddev(CLOSE).over(w) / f.size(f.collect_list(CLOSE).over(w))).alias(
        f"volatility_{window_size_days}_day"
    )


def support_and_resistance(
    window_size_days: int,
    height=None,
    threshold=None,
    distance=None,
    prominence=None,
    width=None,
    wlen=None,
    rel_height=0.5,
    plateau_size=None,
) -> list[Column]:
    """Returns a list of pyspark queries for calculating the basic support and
    resistance. EXPERIMENTAL

    Parameters:
    -----------
        window_size_days: int
            The size of the lookback window in days.
    Returns:
    --------
        Pyspark Column
    """

    get_peaks = f.udf(
        lambda x: list(
            map(
                int,
                find_peaks(
                    x,
                    height=None,
                    threshold=None,
                    distance=None,
                    prominence=None,
                    width=None,
                    wlen=None,
                    rel_height=0.5,
                    plateau_size=None,
                )[0],
            )
        ),
        returnType=ArrayType(IntegerType()),
    )

    w = WindowMaker().create_lookback_window(
        window_size=window_size_days,
        unit_of_time="days",
        order_col=f.col(TIME).cast("long"),
        partition_col=CURRENCY_PAIR,
    )

    upper_bounds = f.greatest(OPEN, CLOSE).alias("upper_bound")
    lower_bounds = f.least(OPEN, CLOSE).alias("lower_bound")

    peak_indeces = get_peaks(f.collect_list(upper_bounds).over(w)).alias("peak_indeces")
    dip_indeces = get_peaks(f.collect_list((-1 * lower_bounds)).over(w)).alias(
        "dip_indeces"
    )

    return [upper_bounds, lower_bounds, peak_indeces, dip_indeces]
