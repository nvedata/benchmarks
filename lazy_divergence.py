from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def main():

    spark: SparkSession = SparkSession.builder.master("local[2]").getOrCreate() # type: ignore
    part_keys = spark.range(10).withColumnRenamed("id", "part_key")
    agg_keys = spark.range(10).withColumnRenamed("id", "agg_key")
    df = part_keys.crossJoin(agg_keys)
    df = df.withColumn("value", F.col("part_key") * 10 + F.col("agg_key"))
    
    # force shuffling
    df = df.repartition(20)
    df, _ = df.randomSplit([0.99, 0.01])
    w_spec = (
        Window
        .partitionBy("part_key")
        .orderBy("value")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing
        )
    )
    parent = df.withColumn("max_agg_key", F.last("agg_key").over(w_spec))
    left = parent.orderBy("part_key") 
    right = parent.orderBy("part_key")

    left_diff = left.exceptAll(right)
    right_diff = right.exceptAll(left)

    assert(left_diff.isEmpty())
    assert(right_diff.isEmpty())


if __name__ == "__main__":
    main()