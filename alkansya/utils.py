import os
from pathlib import Path
from typing import Any

import yaml


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
