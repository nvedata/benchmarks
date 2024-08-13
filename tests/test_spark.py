import datetime
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from utils.spark import annotation_is_list, write_single_csv
from conftest import create_spark_session, dataframe_diff

create_spark_session()

test_annotation_is_type_parameters = [
    (int, False),
    (list, True),
    (list[int], True),
    (list[int | float], True),
    (str, False),
    (list | None, True),
    (list[str] | None, True)
]

@pytest.mark.parametrize("annotation,expected", test_annotation_is_type_parameters)
def test_annotation_is_list(annotation, expected) -> None:
    assert annotation_is_list(annotation) == expected


def test_write_single_csv() -> None:
    # TODO setup and tear down
    spark = SparkSession.getActiveSession()

    path = 'tests/example.csv'
    schema = T.StructType([
        T.StructField('int_value', T.IntegerType()),
        # TODO correct comparison of float
        T.StructField('float_value', T.FloatType()),
        T.StructField('string_value', T.StringType()),
        T.StructField('datetime_value', T.TimestampType())
    ])
    ts = datetime.datetime(2024, 1, 1)
    expected_df = spark.createDataFrame(
        [
            (1, 1.0, 'a', ts),
            (None, 1.0, None, ts),
            (2, None, 'b,c', None)
        ],
        schema
    )
    write_single_csv(expected_df, path)
    df = spark.read.csv(path, header=True, schema=schema)

    left_diff, right_diff = dataframe_diff(df, expected_df)
    assert left_diff.isEmpty() and right_diff.isEmpty()
