import os
import yaml
from pathlib import Path
from typing import Any


class InvalidTargetEnvironment(Exception):
    pass


def get_configurations() -> dict[Any, Any]:
    """Loads the configurations for the scripts based on the target environment."""
    environment = os.environ["ENV"]

    if environment not in ["DEV", "PROD"]:
        raise InvalidTargetEnvironment(
            f"Environment variable `ENV` may only be `DEV` or `PROD`, got {environment}"
        )

    conf = dict(
        yaml.safe_load(
            Path(
                os.path.join(
                    os.path.dirname(Path(__file__).parent),
                    f"configurations/{environment}.yml",
                )
            ).read_text()
        )
    )

    return conf


def convert_window_size_to_seconds(window_size: int, unit_of_time: str):
    """ """
    if window_size < 0:
        raise ValueError("Window size must not be negative.")

    if unit_of_time.lower() == "days":
        multiplier = 86400
    elif unit_of_time.lower() == "hours":
        multiplier = 3600
    elif unit_of_time.lower() == "weeks":
        multiplier = 604800
    else:
        raise ValueError("Invalid unit of time, may only be [`days`, `hours`, `weeks`]")

    return multiplier * window_size
