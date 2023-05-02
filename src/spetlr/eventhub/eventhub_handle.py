import warnings
from datetime import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType

from spetlr.exceptions import IncorrectSchemaException
from spetlr.spark import Spark


class EventHubHandle:
    """
    A handle to read and write from Azure Event Hubs.

    Documentation on Event Hub integration for Pyspark:
        https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md


    Args:
        connection_string (str):
            Connection string for Event Hub from Azure.
        consumer_group (str):
            A consumer group is a view of an entire event hub. Consumer groups enable
            multiple consuming applications to each have a separate view of the event
            stream, and to read the stream independently at their own pace and with
            their own offsets.
        stream (bool):
            Whether to extract/load using in a streaming context.
        receiver_timeout (time):
            The amount of time Event Hub receive calls will be retried before throwing
            an exception.
        operation_timeout (time):
            The amount of time Event Hub API calls will be retried before throwing an
            exception.
        auto_format (bool):
            If True and reading:
                Will automatically extract the 'body' column from json. If the schema
                is provided it will extract the json from the body column.
            If True and loading:
                Will automatically take the input dataframe and put into body column
                and cast to json.
        schema (StructType):
            If provided and reading:
                Will be applied when extracting the json from the body column given
                'auto_format' is set.
            If provided and loading:
                Not applicable.

    Methods:
        read(self):
            Extracts from Event Hub as a dataframe
        append(self, df: DataFrame):
            Loads the input dataframe to the Event Hub.
    """

    def __init__(
        self,
        connection_string: str,
        consumer_group: str = None,
        stream: bool = True,
        receiver_timeout: time = None,
        operation_timeout: time = None,
        auto_format: bool = True,
        schema: StructType = None,
    ):
        self.connection_string = connection_string
        self.consumer_group = consumer_group
        self.stream = stream
        self.receiver_timeout = receiver_timeout
        self.operation_timeout = operation_timeout
        self.auto_format = auto_format
        self.schema = schema
        self.spark = Spark.get()

        spark_context = self.spark.sparkContext

        encrypted_connection_string = (
            spark_context._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                self.connection_string
            )
        )

        self.config = {
            "eventhubs.connectionString": encrypted_connection_string,
        }

    def read(self) -> DataFrame:
        if self.stream:
            reader = self.spark.readStream
        else:
            reader = self.spark.read

        df = reader.format("eventhubs").options(**self.config).load()

        if self.auto_format:
            df = df.withColumn("body", F.col("body").cast(StringType()))
        if self.schema:
            df = df.withColumn("body", F.from_json(F.col("body"), self.schema))
        else:
            warnings.warn(
                """
                Auto formatting was applied, but 'schema' was not supplied so the 'body'
                column cannot be extracted from json. Consider providing 'schema' in
                your configuration.
                """
            )

        return df

    def append(self, df: DataFrame) -> None:
        """Loads the input dataframe to the Event Hub."""
        print("Loading to event hub")

        if self.receiver_timeout:
            self.config["eventhubs.receiverTimeout"] = self.receiver_timeout.strftime(
                "PT%HH%MM%SS"
            )
        if self.operation_timeout:
            self.config["eventhubs.operationTimeout"] = self.operation_timeout.strftime(
                "PT%HH%MM%SS"
            )

        if self.auto_format:
            df = (
                df.withColumn("body", F.struct(df.columns))
                .withColumn("body", F.to_json("body"))
                .select("body")
            )
        elif not self.auto_format and df.columns != ["body"]:
            raise IncorrectSchemaException(
                """
                The input df should only have a single column named 'body'
                Note that data in the 'body' column should be formatted to json
                """
            )

        if self.stream:
            writer = df.writeStream
        else:
            writer = df.write

        writer.format("eventhubs").options(**self.config).save()

    def overwrite(self, df: DataFrame) -> None:
        warnings.warn(
            """
            Loading to an event hub has no 'overwrite' mode
            Loading mode is changed to 'append'
            Consider changing the load mode to 'append' in your configuration
            """
        )

        self.append(df)
