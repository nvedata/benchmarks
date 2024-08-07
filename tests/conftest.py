from pyspark.sql import SparkSession, DataFrame

SparkSession.builder.master("local").getOrCreate()


def dataframe_diff(left: DataFrame, right: DataFrame) -> tuple[DataFrame, DataFrame]:
    left_diff = left.exceptAll(right)
    right_diff = right.exceptAll(left)
    return left_diff, right_diff
