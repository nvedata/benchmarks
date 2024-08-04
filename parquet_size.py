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


spark: SparkSession = SparkSession.builder.master("local").getOrCreate()

df = create_rand_df(10**7, 2)
df = df.repartition(2, 'id')
dir_name = 'repart_2_and_col'
df.write.parquet(dir_name, mode='overwrite')
print(dir_name)
print_parquet_stats(dir_name)

df = create_rand_df(10**7, 2)
df = df.repartition(2)
dir_name = 'repart_2'
df.write.parquet(dir_name, mode='overwrite')
print(dir_name)
print_parquet_stats(dir_name)
