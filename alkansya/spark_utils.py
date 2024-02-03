from pyspark.sql import Column, Window


class WindowMaker:
    resolution = {
        "days": 86400,
        "hours": 3600,
        "weeks": 604800,
        "minutes": 60,
    }

    @staticmethod
    def convert_window_size_to_seconds(window_size: int, unit_of_time: str) -> int:
        """ """
        if window_size < 0:
            raise ValueError("Window size must not be negative.")

        multiplier = WindowMaker.resolution.get(unit_of_time, None)

        if multiplier is None:
            raise ValueError(
                f"Unit of time must be one of: {WindowMaker.resolution.keys()}"
            )

        return multiplier * window_size

    @staticmethod
    def create_lookback_window(
        window_size: int,
        unit_of_time: str,
        order_col: list[str] | str | Column,
        partition_col: list[str] | str | Column,
    ):
        """ """

        lookback_window_size = WindowMaker.convert_window_size_to_seconds(
            window_size=window_size, unit_of_time=unit_of_time
        )

        w = (
            Window.partitionBy(partition_col)
            .orderBy(order_col)
            .rangeBetween(start=-lookback_window_size, end=Window.currentRow)
        )

        return w
