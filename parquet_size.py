from dataclasses import dataclass, fields
import itertools
from functools import reduce
import os
from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession, DataFrame, Column, DataFrameWriter
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from utils.spark import annotations_to_schema, write_single_csv, write_schema, read_schema


@dataclass
class Point:
    '''Point of parameters.

    Notes
    -----
    Attribute annotations define Spark DataFrame schema.
    Handle with care.
    '''
    n_rows: int
    fractions: list[int | float]
    part_mode: str
    n_part: int | None
    part_cols: list[str] | None
    # buckets: list[int, list[str]]
    # sort_cols: list[str] | None

    def to_tuple(self) -> tuple:
        # format Point because existing parameters are formatted
        params_f = format_params(self)
        return tuple(params_f.values())


class Dimensions(Point):

    n_rows: list[int]
    fractions: list[list[int | float]]
    part_mode: list[str]
    n_part: list[int | None]
    part_cols: list[list[str] | None]
    # buckets: list[list[int, list[str]]]
    # sort_cols: list[list[str] | None]

    @property
    def grid_iterator(self) -> Iterable:
        return itertools.product(*vars(self).values())


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


def points_from_dataframe(df: DataFrame, point_class: type) -> set[tuple]:
    param_cols = [field.name for field in fields(point_class)]
    point_set = set(
        tuple(row.asDict().values()) 
        for row in df.select(param_cols).collect()
    )
    return point_set


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
    fractions: list[int | float]
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
        if n_part is None and cols is None:
            df = df
        elif n_part is None:
            df = df.repartition(*cols)
        elif cols is None:
            df = df.repartition(n_part)
        else:
            df = df.repartition(n_part, *cols)
        return df
    
    elif mode == 'coalesce':
        if n_part is None:
            df = df
        else:
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
    bucket_cols: list[str] = None,
    sort_cols: list[str] = None
    ) -> DataFrameWriter:

    if n_buckets is None != bucket_cols is None:
        raise ValueError(
            'Both `n_buckets` and `bucket_cols` must be `None`, '
            'or both must be specified'
        )
    
    if n_buckets is None:
        return  df.write
    else:
        writer = df.write.bucketBy(n_buckets, *bucket_cols)

    if sort_cols is not None:
        writer = writer.sortBy(sort_cols)

    return writer


def set_writer_sorting(
    writer: DataFrameWriter,
    sort_cols: list[str] = None
    ) -> DataFrameWriter:

    if sort_cols is None:
        return writer
    else:
        writer.sortBy(sort_cols)


def get_case_stat(params: Point, dir_name: str) -> DataFrame:

    spark = SparkSession.getActiveSession()
    stats_df = get_parquet_stats(dir_name)
    params_f = format_params(params)
    schema = annotations_to_schema(Point)
    case_df : DataFrame = spark.createDataFrame([params_f], schema=schema)
    case_df = case_df.join(stats_df, F.lit(True))
    
    return case_df


def format_params(params: Point) -> dict[str, object]:
    '''Format parameters values.
    Convert lists to strings.

    Parameters
    ----------
    params: Point
        Point of parameters.

    Returns
    -------
    dict[str, object]
        Formatted parameters.
    '''

    f_params = {}
    for key, val in vars(params).items():
        # if val is None:
        #     val = 'NONE'
        if isinstance(val, list):
            val = str(val)

        f_params[key] = val

    return f_params


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

    # TODO point iterators to sets, set union and difference
    dimensions = {
        "n_rows": [
            10 ** 6,
            # 10 ** 7
        ],
        "fractions": [
            [1, 1],
            [99, 1]
        ],
        # TODO coalesce
        "part_mode": [
            "repartition"
        ],
        "n_part": [None, 2],
        "part_cols": [
            None,
            ["id"]
        ]
    }

    report_path = 'parquet_size_report'

    stats = []
    dims = Dimensions(**dimensions)

    try:
        schema = read_schema(f'{report_path}.json')
        report = spark.read.csv(f'{report_path}.csv', header=True, schema=schema)
        dir_num = report.agg(F.max('dir_name')).first()[0] + 1
        existing_points = points_from_dataframe(report, Point)
    except Exception:
        dir_num = 0
        existing_points = set()
    
    for point in dims.grid_iterator:
        
        p = Point(*point)
        param_set_exists = p.to_tuple() in existing_points
        if param_set_exists:
            continue

        dir_name = f'data/{dir_num}'
        dir_num += 1
        
        print(vars(p))

        # parametrized functions
        df = create_skewed_df(p.n_rows, p.fractions)
        df = partitioning(df, p.part_mode, p.n_part, p.part_cols)
        # bucketing is not supported for parquet
        # writer = get_bucketing_writer(df, *p.buckets, p.sort_cols)
        df.write.parquet(dir_name, mode='overwrite')

        # get statistics
        case_stat_df = get_case_stat(p, dir_name)
        stats.append(case_stat_df)

    if stats:
        report = reduce(DataFrame.union, stats)
        report = report.withColumn('dir_name', F.col('dir_name').cast(IntegerType()))
        report = report.orderBy("dir_name", "part_name", "id")
        write_single_csv(report, f'{report_path}.csv', mode='append')
        write_schema(report.schema, f'{report_path}.json')


if __name__ == "__main__":
    main()
