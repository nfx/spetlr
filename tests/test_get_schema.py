import unittest
from textwrap import dedent

from atc.sql.schema import get_schema
from pyspark.sql import types as t


class TestGetSchema(unittest.TestCase):
    def test_01_schema1(self):
        sql = dedent(
            r"""
        (
            a int,
            b int COMMENT "really?\" is that it?",
            c string,
            cplx struct< -- irrelevant comment
                someId:string,
                QrCode:string,
                details:struct/* why?! */<id:string>,
                blabla : array< int >
                >,
            d timestamp,
            m map<int,string>,
            p decimal(10,3)
        )
        """
        )
        struct = get_schema(sql)
        self.assertEqual(
            t.StructType(
                [
                    t.StructField("a", t.IntegerType(), True),
                    t.StructField("b", t.IntegerType(), True),
                    t.StructField("c", t.StringType(), True),
                    t.StructField(
                        "cplx",
                        t.StructType(
                            [
                                t.StructField("someId", t.StringType(), True),
                                t.StructField("QrCode", t.StringType(), True),
                                t.StructField(
                                    "details",
                                    t.StructType(
                                        [t.StructField("id", t.StringType(), True)]
                                    ),
                                    True,
                                ),
                                t.StructField(
                                    "blabla", t.ArrayType(t.IntegerType(), True), True
                                ),
                            ]
                        ),
                        True,
                    ),
                    t.StructField("d", t.TimestampType(), True),
                    t.StructField(
                        "m", t.MapType(t.IntegerType(), t.StringType(), True), True
                    ),
                ]
            ),
            struct,
        )


if __name__ == "__main__":
    unittest.main()