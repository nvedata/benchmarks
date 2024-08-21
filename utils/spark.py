import json
from pathlib import Path
import subprocess
import tempfile
from types import UnionType
from typing import get_origin, get_args, get_type_hints

from pyspark.sql import DataFrame
from pyspark.sql import types as T

def to_pyspark_type(py_type: type) -> T.DataType:

    # explicit equality comparison, unlike in match-case
    if py_type is bool: return T.BooleanType  # noqa: E701
    elif py_type is int: return T.IntegerType  # noqa: E701
    elif py_type is float: return T.FloatType  # noqa: E701
    elif py_type is str: return T.StringType  # noqa: E701
    # elif py_type == list: return T.ArrayType 
    else: raise TypeError('unable to map the type')  # noqa: E701


def annotation_is_list(annotation: object) -> bool:

    origin = get_origin(annotation)
    if origin is None:
        return annotation is list
    elif origin == UnionType:
        checks = []
        for arg in get_args(annotation):
            checks.append(annotation_is_list(arg))
        return any(checks)
    else:
        return origin is list


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
            nullable = ant_args[-1] is type(None)
        elif ant_args:
            type_ = ant_args[0]
            nullable = ant_args[-1] is type(None)
        else:
            type_ = ant


        field = T.StructField(name, to_pyspark_type(type_)(), nullable)
        fields.append(field)

    schema = T.StructType(fields)
    return schema


def write_single_csv(df: DataFrame, path: str, mode: str) -> None:

    with tempfile.TemporaryDirectory() as temp_path:
        df.coalesce(1).write.csv(temp_path, header=True, mode='overwrite')
        csv_path = next(Path(temp_path).glob('*.csv'))

        if mode == 'overwrite':
            csv_path.replace(path)

        elif mode == 'append':
            if Path(path).exists():
                # TODO schema check
                subprocess.run(f'tail -n +2 {str(csv_path)} >> {path}', shell=True)
            else:
                csv_path.replace(path)

        else:
            raise ValueError(f'Unknown mode {mode}')


def write_schema(schema: T.StructType, path: str) -> None:

    schema_dict = json.loads(schema.json())
    with open(path, 'w') as file:
        json.dump(schema_dict, file, indent=4)


def read_schema(path: str) -> T.StructType:

    with open(path) as file:
        schema_dict = json.load(file)

    schema = T.StructType.fromJson(schema_dict)
    return schema
