import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark: SparkSession = SparkSession.builder.master("local").getOrCreate()

df = spark.range(10**8)
df = df.withColumn("id", F.round(F.rand()).cast(IntegerType()))
df = df.repartition(2, 'id')
dir_name = '2_partitions'
df.write.parquet(dir_name, mode='overwrite')

dir_path = Path(dir_name)
parquet_size = [
    os.path.getsize(dir_path / i) 
    for i in os.listdir(dir_name) if i.endswith('.parquet')
]
print(parquet_size)
