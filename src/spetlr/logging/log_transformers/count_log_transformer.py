from pyspark.sql import DataFrame

from spetlr.etl.types import dataset_group
from spetlr.logging import LogTransformer


class CountLogTransformer(LogTransformer):
    def log(self, df: DataFrame) -> dict:
        key = self.dataset_input_key_list[0]
        n_rows = df.count()

        results = {
            key: {
                "n_rows": n_rows,
            },
        }

        return results

    def log_many(self, dataset: dataset_group) -> dict:
        results = {}

        for key, df in dataset.items():
            n_rows = df.count()
            results.update(
                {
                    key: {
                        "n_rows": n_rows,
                    },
                }
            )

        return results
