from abc import ABC, abstractmethod
from datetime import datetime, timezone
from json import dumps
from typing import List, Union
from uuid import uuid4

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from spetlr.etl import TransformerNC
from spetlr.etl.types import dataset_group
from spetlr.spark import Spark


class LogTransformer(ABC, TransformerNC):
    def __init__(
        self,
        *,
        log_name: str,
        column: str = None,
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
        **kwargs,
    ):
        self.log_name = log_name
        self.column = column
        self.timestamp_now = datetime.now(tz=timezone.utc)

        if dataset_output_key is None:
            dataset_output_key = str(uuid4())
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
        )
        self.log_schema = StructType(
            [
                StructField("Id", StringType(), True),
                StructField("LogName", StringType(), True),
                StructField("Timestamp", TimestampType(), True),
                StructField("LogMethodName", StringType(), True),
                StructField("DatasetInputKeys", StringType(), True),
                StructField("ColumnName", StringType(), True),
                StructField("Results", StringType(), True),
                StructField("Arguments", StringType(), True),
            ]
        )
        self.spark = Spark().get()
        self.df_log: DataFrame = self.spark.createDataFrame(
            data=[], schema=self.log_schema
        )
        self.log_method_name = self.__class__.__name__
        self.kwargs = kwargs

    @abstractmethod
    def log(self, df: DataFrame) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def log_many(self, dataset: dataset_group) -> dict:
        raise NotImplementedError()

    def process(self, df: DataFrame) -> DataFrame:
        dataset_input_keys = [(self.dataset_input_key_list[0])]
        results = self.log(df)

        return self._create_log(results, self.kwargs, dataset_input_keys)

    def process_many(self, datasets: dataset_group) -> DataFrame:
        dataset_input_keys = list(datasets.keys())
        results = self.log_many(datasets)

        return self._create_log(results, self.kwargs, dataset_input_keys)

    def _create_log(
        self, results: dict, arguments: dict, dataset_input_keys: List[str]
    ) -> DataFrame:
        results = dumps(results)
        arguments = dumps(arguments)
        dataset_input_keys = dumps(dataset_input_keys)

        log_data = [
            (
                None,  # Id
                self.log_name,  # LogName
                self.timestamp_now,  # Timestamp
                self.log_method_name,  # LogMethodName
                dataset_input_keys,  # DatasetInputKeys
                self.column,  # ColumnName
                results,  # Results
                arguments,  # Arguments
            )
        ]

        df = self.spark.createDataFrame(data=log_data, schema=self.log_schema)

        columns_for_id = [
            "LogName",
            "Timestamp",
            "LogMethodName",
        ]

        if self.column:
            columns_for_id.append("ColumnName")

        df = df.withColumn(
            "Id",
            F.md5(F.concat(*columns_for_id)),
        )

        return self.df_log.union(df)
