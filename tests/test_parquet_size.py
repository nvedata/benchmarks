from pyspark.sql import SparkSession

from parquet_size import column_rank
from conftest import dataframe_diff

from pyspark.sql import functions as F

def test_column_rank():
    spark = SparkSession.getActiveSession()
    df = spark.range(5)
    df = df.withColumn('rank', column_rank(df['id'], [1, 3]))
    expected_df = spark.createDataFrame(
        [
            [0, 0],
            [1, 0],
            [2, 1],
            [3, 1],
            [4, 2]
        ],
        ['id', 'rank']
    )
    left_diff, right_diff = dataframe_diff(df, expected_df)
    assert left_diff.isEmpty() and right_diff.isEmpty()

if __name__ == '__main__':
    test_column_rank()
