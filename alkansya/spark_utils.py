from pyspark.sql import Column, Window


class WindowMaker:
    window_size_multipliers = {
        "days": 86400,
        "hours": 3600,
        "weeks": 604800,
        "minutes": 60,
    }

    def __init__(self) -> None:
        """ """

    def convert_window_size_to_seconds(
        self, window_size: int, unit_of_time: str
    ) -> int:
        """ """
        if window_size < 0:
            raise ValueError("Window size must not be negative.")

        multiplier = self.window_size_multipliers.get(unit_of_time, None)

        if multiplier is None:
            raise ValueError(
                f"Unit of time must be one of: {self.window_size_multipliers.keys()}"
            )

        return multiplier * window_size

    def create_lookback_window(
        self,
        window_size: int,
        unit_of_time: str,
        order_col: list[str] | str | Column,
        partition_col: list[str] | str | Column,
    ):
        """ """

        lookback_window_size = self.convert_window_size_to_seconds(
            window_size, unit_of_time
        )

        w = (
            Window.partitionBy(partition_col)
            .orderBy(order_col)
            .rangeBetween(-lookback_window_size, Window.currentRow)
        )

        return w
