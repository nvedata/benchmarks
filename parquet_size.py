import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark: SparkSession = SparkSession.builder.master("local").getOrCreate()

df = spark.range(10**7)
df = df.withColumn("id", F.round(F.rand()).cast(IntegerType()))

df = df.repartition(2, 'id')
dir_name = 'repart_2_and_col'
df.write.parquet(dir_name, mode='overwrite')

dir_path = Path(dir_name)
parquet_paths = dir_path.glob('**/*.parquet')
for path in parquet_paths:
    print('file size:', os.path.getsize(path))
    df_part = spark.read.parquet(str(path))
    df_part.groupby('id').agg(F.count('id')).show()
