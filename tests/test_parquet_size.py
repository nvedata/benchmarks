from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from parquet_size import column_rank, create_skewed_df
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


def show_skewed_df():
    df = create_skewed_df(10 ** 6, [1, 1, 2, 3, 5])
    gr_df = df.groupby('id').agg(F.count('id').alias('count'))
    gr_df.show()


if __name__ == '__main__':
    test_column_rank()
    show_skewed_df()
