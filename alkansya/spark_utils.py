from pyspark.sql import Column, Window


class TemporalWindowMaker:
    resolution = {
        "days": 86400,
        "hours": 3600,
        "weeks": 604800,
        "minutes": 60,
    }

    @staticmethod
    def convert_window_size_to_seconds(window_size: int, unit_of_time: str) -> int:
        """Return the number of seconds inside a given window size."""
        if window_size < 0:
            raise ValueError("Window size must not be negative.")

        multiplier = TemporalWindowMaker.resolution.get(unit_of_time, None)

        if multiplier is None:
            raise ValueError(
                f"Unit of time must be one of: {TemporalWindowMaker.resolution.keys()}"
            )

        return multiplier * window_size

    @staticmethod
    def create_lookback_window(
        window_size: int,
        unit_of_time: str,
        order_col: list[str] | str | Column,
        partition_col: list[str] | str | Column,
    ):
        """Return a pyspark `WindowSpec` object that's configured to do a partitioned
        lookback that is time aware.
        """

        lookback_window_size = TemporalWindowMaker.convert_window_size_to_seconds(
            window_size=window_size, unit_of_time=unit_of_time
        )

        w = (
            Window.partitionBy(partition_col)
            .orderBy(order_col)
            .rangeBetween(start=-lookback_window_size, end=Window.currentRow)
        )

        return w
