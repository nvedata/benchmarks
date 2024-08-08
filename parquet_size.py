import itertools
from functools import reduce
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Column, DataFrameWriter
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


def create_skewed_df(
    n_rows: int,
    fractions: list[float]
    ) -> DataFrame:

    spark = SparkSession.getActiveSession()

    frac_sum = sum(fractions)
    norm_fractions = (frac / frac_sum for frac in fractions)
    cumulative_norm_fractions = itertools.accumulate(norm_fractions)

    df = spark.range(n_rows)
    df = df.withColumn('rand', F.rand()).cache()
    rank_col = column_rank(F.col('rand'), cumulative_norm_fractions)
    df = df.select(rank_col.alias('id'))

    return df


def column_rank(col: Column, iterable: list) -> Column:
    '''Return column rank within list.

    Parameters
    ----------
        col : Column
            Column with numeric values.

        list_ : list_
            Sorted list of numeric values.
    '''

    rank_col = F.lit(0)
    for i, value in enumerate(iterable):
        rank_col = F.when(col <= value, rank_col).otherwise(i + 1)

    return rank_col


def partitioning(
    df: DataFrame,
    mode: str,
    n_part: int = None,
    cols: list[str] = None
    ) -> DataFrame:

    if mode == 'repartition':
        if n_part is None:
            df = df.repartition(*cols)
        elif cols is None:
            df = df.repartition(n_part)
        else:
            df = df.repartition(n_part, *cols)
        return df
    
    elif mode == 'coalesce':
        df = df.coalesce(n_part)
        return df

    else:
        raise ValueError(
            'Unknown `mode` value. Available values: '
            '`repartition`, `coalesce`'
        )
    

def get_bucketing_writer(
    df: DataFrame,
    n_buckets: int = None,
    bucket_cols: list[str] = None
    ) -> DataFrameWriter:

    if n_buckets is None != bucket_cols is None:
        raise ValueError(
            'Both `n_buckets` and `bucket_cols` must be `None`, '
            'or both must be specified'
        )
    
    if n_buckets is None:
        return df.write
    else:
        return df.write.bucketBy(n_buckets, *bucket_cols)


def set_writer_sorting(
    writer: DataFrameWriter,
    sorting_cols: list[str] = None
    ) -> DataFrame:

    if sorting_cols is None:
        return writer
    else:
        writer.sortBy()


def get_case_stat(params: dict) -> DataFrame:

    spark = SparkSession.getActiveSession()
    stats_df = get_parquet_stats(self.name)
    case_df : DataFrame = spark.createDataFrame([params])
    case_df = case_df.join(stats_df, F.lit(True))
    
    return case_df


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

    # TODO grids sum

    n_rows_dim = [10 ** 6, 10 ** 7]
    fractions_dim = [[1, 1], [99, 1]]
    # TODO grid sum

    # TODO bucket
    # TODO sortby
    # TODO coalesce
    part_mode_dim = ['repartition']
    n_part_dim = [2, None]
    cols_dim = [["id"], None]

    stats = []
    for params in itertools.product(
        fractions_dim,
        n_rows_dim,
        part_mode_dim,
        n_part_dim,
        cols_dim
    ):
        fractions, n_rows, mode, n_part, cols = params
        # df = create_skewed_df(n_rows, fractions)
        # df = partitioning(df, mode, n_part, cols)
        # writer = get_bucketing_writer(df, n_buckets, bucket_cols)
        # writer = set_writer_sorting(writer, sorting_cols)
        # stats = get_case_stat(df)

    
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

    # TODO parquet cleanup option
if __name__ == "__main__":
    main()
