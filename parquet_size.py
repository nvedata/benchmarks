from functools import reduce
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


class PartitioningCase:

    def __init__(
        self,
        name: str,
        n_rows: int,
        n_unique: int,
        n_part: int = None,
        cols: list = None
        ):

        self.name = name
        self.n_rows = n_rows
        self.n_unique = n_unique
        self.n_part = n_part
        self.cols = cols


    def repartition(self, df: DataFrame) -> DataFrame:

        if self.n_part is None:
            df = df.repartition(*self.cols)
        elif self.cols is None:
            df = df.repartition(self.n_part)
        else:
            df = df.repartition(self.n_part, *self.cols)
        return df


    def run(self) -> DataFrame:

        spark = SparkSession.getActiveSession()
        df = create_rand_df(self.n_rows, self.n_unique)
        df = self.repartition(df)
        df.write.parquet(self.name, mode="overwrite")
        stats_df = get_parquet_stats(self.name)
        case_df : DataFrame = spark.createDataFrame([{
            "n_rows": self.n_rows,
            "n_unqiue": self.n_unique,
        }])
        case_df = case_df.join(stats_df, F.lit(True))
        
        return case_df


def create_rand_df(
    n_rows: int,
    n_unique: int
    ) -> DataFrame:

    spark = SparkSession.getActiveSession()
    df = spark.range(n_rows)
    df = df.withColumn(
        "id", 
        F.round(
            F.rand() * (n_unique - 1)
        ).cast(IntegerType())
    )
    return df


def print_parquet_stats(dir_name: str) -> None:

    spark = SparkSession.getActiveSession()
    dir_path = Path(dir_name)
    parquet_paths = dir_path.glob("**/*.parquet")
    for path in parquet_paths:
        print("file size:", os.path.getsize(path))
        df_part = spark.read.parquet(str(path))
        df_part.groupby("id").agg(F.count("id")).show()


def get_parquet_stats(dir_name: str) -> DataFrame:

    spark = SparkSession.getActiveSession()
    dir_path = Path(dir_name)
    parquet_paths = dir_path.glob("**/*.parquet")
    parquet_paths
    stats = []
    for path in parquet_paths:
        stat_df : DataFrame = spark.createDataFrame([{
            "dir_name": dir_name,
            "part_name": path.name.split("-")[1],
            "file_size": os.path.getsize(path)
        }])

        part_df = spark.read.parquet(str(path))
        groups_df = part_df.groupby("id").agg(
            F.count("id").alias("count")
        )
        stat_df = stat_df.join(groups_df, F.lit(True), how="left")
        stats.append(stat_df)

    stats_df = reduce(DataFrame.union, stats)
    return stats_df


def main():
    spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
    cases = [
        PartitioningCase("repart_1m_p2", 10**6, 2, n_part=2),
        PartitioningCase("repart_1m_p2_cols", 10**6, 2, n_part=2, cols=['id']),
        PartitioningCase("repart_1m_cols", 10**6, 2, cols=['id']),
        PartitioningCase("repart_10m_p2", 10**7, 2, n_part=2),
        PartitioningCase("repart_10m_p2_cols", 10**7, 2, n_part=2, cols=['id']),
        PartitioningCase("repart_10m_cols", 10**7, 2, cols=['id'])
    ]

    stats = []
    for case_ in cases:
        stats_df = case_.run()
        stats.append(stats_df)

    stats_df = reduce(DataFrame.union, stats)
    stats_df.orderBy("dir_name", "part_name", "id").show()


if __name__ == "__main__":
    main()
