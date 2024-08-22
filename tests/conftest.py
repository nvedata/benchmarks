from pyspark.sql import SparkSession, DataFrame

def create_spark_session():
    SparkSession.builder.master("local").getOrCreate() # type: ignore


def dataframe_diff(left: DataFrame, right: DataFrame) -> tuple[DataFrame, DataFrame]:
    left_diff = left.exceptAll(right)
    right_diff = right.exceptAll(left)
    return left_diff, right_diff
