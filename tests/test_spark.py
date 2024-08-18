import datetime
import json
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from utils.spark import annotation_is_list, write_single_csv, write_schema, read_schema
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
    write_single_csv(expected_df, path, 'overwrite')
    df = spark.read.csv(path, header=True, schema=schema)

    left_diff, right_diff = dataframe_diff(df, expected_df)
    assert left_diff.isEmpty() and right_diff.isEmpty()


def test_write_single_csv_append() -> None:

    spark = SparkSession.getActiveSession()

    path = 'tests/example.csv'
    schema = T.StructType([
        T.StructField('int_value', T.IntegerType()),
        T.StructField('float_value', T.FloatType()),
        T.StructField('string_value', T.StringType()),
        T.StructField('datetime_value', T.TimestampType())
    ])
    ts = datetime.datetime(2024, 1, 1)

    # first write
    expected_df = spark.createDataFrame(
        [
            (1, 1.0, 'a', ts),
            (None, 1.0, None, ts),
            (2, None, 'b,c', None)
        ],
        schema
    )
    write_single_csv(expected_df, path, 'append')
    df = spark.read.csv(path, header=True, schema=schema)

    left_diff, right_diff = dataframe_diff(df, expected_df)
    assert left_diff.isEmpty() and right_diff.isEmpty()

    # second write
    append_df = spark.createDataFrame(
        [
            (None, 2.0, 'b,c', ts)
        ],
        schema
    )
    write_single_csv(append_df, path, 'append')

    expected_df = expected_df.union(append_df)
    left_diff, right_diff = dataframe_diff(df, expected_df)
    assert left_diff.isEmpty() and right_diff.isEmpty()


def test_write_schema() -> None:

    spark = SparkSession.getActiveSession()

    path = 'tests/example.json'
    expected_schema = T.StructType([
        T.StructField('int_value', T.IntegerType()),
        T.StructField('float_value', T.FloatType()),
        T.StructField('string_value', T.StringType()),
        T.StructField('datetime_value', T.TimestampType())
    ])

    write_schema(expected_schema, path)
    # TODO assert with read_schema
    schema = read_schema(path)

    assert schema == expected_schema
