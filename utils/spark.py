from pathlib import Path
import tempfile
from types import UnionType
from typing import get_origin, get_args, get_type_hints

from pyspark.sql import DataFrame
from pyspark.sql import types as T

def to_pyspark_type(py_type: type) -> T.DataType:

    # explicit equality comparison, unlike in match-case
    if py_type == bool: return T.BooleanType
    elif py_type == int: return T.IntegerType
    elif py_type == float: return T.FloatType
    elif py_type == str: return T.StringType
    # elif py_type == list: return T.ArrayType 
    else: raise TypeError('unable to map the type')


def annotation_is_list(annotation: object) -> bool:

    origin = get_origin(annotation)
    if origin is None:
        return annotation == list
    elif origin == UnionType:
        checks = []
        for arg in get_args(annotation):
            checks.append(annotation_is_list(arg))
        return any(checks)
    else:
        return origin == list


def annotations_to_schema(dataclass: type) -> T.StructType:

    annotations = get_type_hints(dataclass)
    fields = []
    for name, ant in annotations.items():

        nullable = False
        ant_args = get_args(ant)
        # list is converted to str
        # TODO support of list and dict types
        if annotation_is_list(ant):
            type_ = str
            nullable = ant_args[-1] == type(None)
        elif ant_args:
            type_ = ant_args[0]
            nullable = ant_args[-1] == type(None)
        else:
            type_ = ant


        field = T.StructField(name, to_pyspark_type(type_)(), nullable)
        fields.append(field)

    schema = T.StructType(fields)
    return schema


def write_single_csv(df: DataFrame, path: str) -> None:
    with tempfile.TemporaryDirectory() as temp_path:
        df.coalesce(1).write.csv(temp_path, header=True, mode='overwrite')
        csv_path = next(Path(temp_path).glob('*.csv'))
        csv_path.replace(path)
