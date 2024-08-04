from functools import reduce
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def create_rand_df(
    n_rows: int,
    n_unique: int
    ) -> DataFrame:

    df = spark.range(n_rows)
    df = df.withColumn(
        "id", 
        F.round(
            F.rand() * (n_unique - 1)
        ).cast(IntegerType())
    )
    return df


def print_parquet_stats(dir_name: str) -> None:
    dir_path = Path(dir_name)
    parquet_paths = dir_path.glob('**/*.parquet')
    for path in parquet_paths:
        print('file size:', os.path.getsize(path))
        df_part = spark.read.parquet(str(path))
        df_part.groupby('id').agg(F.count('id')).show()


def get_parquet_stats(dir_name: str) -> list[DataFrame]:

    dir_path = Path(dir_name)
    parquet_paths = dir_path.glob('**/*.parquet')
    parquet_paths
    stats = []
    for path in parquet_paths:
        stat_df : DataFrame = spark.createDataFrame([{
            'dir_name': dir_name,
            'part_name': path.name.split('-')[1],
            'file_size': os.path.getsize(path)
        }])

        part_df = spark.read.parquet(str(path))
        groups_df = part_df.groupby('id').agg(
            F.count('id').alias('count')
        )
        stat_df = stat_df.join(groups_df, F.lit(True), how='left')
        stats.append(stat_df)

    return stats


spark: SparkSession = SparkSession.builder.master("local").getOrCreate()

stats = []
df = create_rand_df(10**6, 2)
df = df.repartition(2, 'id')
dir_name = 'repart_2_and_col'
df.write.parquet(dir_name, mode='overwrite')
stats.extend(get_parquet_stats(dir_name))

df = create_rand_df(10**6, 2)
df = df.repartition(2)
dir_name = 'repart_2'
df.write.parquet(dir_name, mode='overwrite')
stats.extend(get_parquet_stats(dir_name))

stats_df = reduce(DataFrame.union, stats)
stats_df.orderBy('dir_name', 'part_name', 'id').show()
