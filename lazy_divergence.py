from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.spark import write_single_csv

def main():

    spark: SparkSession = SparkSession.builder.master("local[2]").getOrCreate() # type: ignore
    part_keys = spark.range(10).withColumnRenamed("id", "part_key")
    sort_keys = spark.range(10).withColumnRenamed("id", "sort_key")
    df = part_keys.crossJoin(sort_keys)
    df = df.withColumn("value", F.col("part_key") * 10 + F.col("sort_key"))
    # set the same sort keys to randomized output
    df = df.withColumn("sort_key", F.lit(1))
    
    # force shuffling
    df = df.repartition(20)
    df, _ = df.randomSplit([0.99, 0.01])
    w_spec = (
        Window
        .partitionBy("part_key")
        .orderBy("sort_key")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing
        )
    )
    parent = df.withColumn("max_value", F.last("value").over(w_spec))

    left = parent.orderBy("part_key")
    write_single_csv(left, "data/left.csv", mode="overwrite")
    
    right = parent.orderBy("part_key")
    write_single_csv(left, "data/right.csv", mode="overwrite")

    left = spark.read.csv('data/left.csv')
    right = spark.read.csv('data/right.csv')

    left_diff = left.exceptAll(right)
    right_diff = right.exceptAll(left)

    assert(left_diff.isEmpty())
    assert(right_diff.isEmpty())


if __name__ == "__main__":
    main()