# Script containing custom transformers to be used in a `pyspark.ml.pipeline`.

from alkansya.feature_engineering import spark_query as query
from typing import Optional
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Param,
    Params,
    TypeConverters,
)
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from alkansya.feature_engineering.spark_query import ohlcv


class OHLCVTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """ """

    resolution_in_minutes = Param(
        Params._dummy(),
        "resolution_in_minutes",
        "Resolution to use for aggregating the OHLCV statistics.",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self, resolution_in_minutes: int):
        super(OHLCVTransformer, self).__init__()

        kwargs = self._input_kwargs()
        self.setParams(**kwargs)

    def setParams(self):
        kwargs = self._input_kwargs()
        return self.setParams(**kwargs)

    def _transform(self, dfs):
        """ """
        ohlcv
