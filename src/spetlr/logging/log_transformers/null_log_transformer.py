from pyspark.sql import DataFrame

from spetlr.etl.types import dataset_group
from spetlr.logging import LogTransformer


class NullLogTransformer(LogTransformer):
    def log(self, df: DataFrame) -> dict:
        self._check_args()

        key = self.dataset_input_key_list[0]

        n_rows = df.count()
        n_nulls = df.filter(df[self.column].isNull()).count()
        percentage_of_total = round(n_nulls / n_rows, 4)

        results = {
            key: {
                "n_nulls": n_nulls,
                "percentage_of_total": percentage_of_total,
            },
        }

        return results

    def log_many(self, dataset: dataset_group) -> dict:
        self._check_args()

        results = {}

        for key, df in dataset.items():
            n_rows = df.count()
            n_nulls = df.filter(df[self.column].isNull()).count()
            percentage_of_total = round(n_nulls / n_rows, 4)

            results.update(
                {
                    key: {
                        "n_rows": n_rows,
                        "n_nulls": n_nulls,
                        "percentage_of_total": percentage_of_total,
                    }
                }
            )

        return results

    def _check_args(self) -> None:
        if not self.column:
            raise ValueError("The argument 'column' is missing")
