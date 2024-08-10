from pyspark.sql import types as T
from types import GenericAlias, UnionType
from typing import get_origin, get_args, get_type_hints

def to_pyspark_type(py_type: type) -> T.DataType:

    # explicit equality comparison, unlike in match-case
    if py_type == bool: return T.BooleanType
    elif py_type == int: return T.IntegerType
    elif py_type == float: return T.FloatType
    elif py_type == str: return T.StringType
    # elif py_type == list: return T.ArrayType 
    else: raise TypeError('unable to map the type')


def annotation_is_type(annotation: object, type_: type) -> bool:

    if annotation == list:
        return True
    elif get_origin(annotation) == list:
        return True
    else:
        return False


def annotations_to_schema(dataclass) -> T.StructType:

    annotations = get_type_hints(dataclass)
    fields = []
    for name, ant in annotations.items():

        nullable = False
        ant_args = get_args(ant)
        # list is converted to str
        # TODO support of list and dict types
        # TODO fix for union, i.e. list[str] | None
        if annotation_is_type(ant, list):
            type_ = str
        elif ant_args:
            type_ = ant_args[0]
            nullable = ant_args[-1] is None
        else:
            type_ = ant


        field = T.StructField(name, to_pyspark_type(type_)(), nullable)
        fields.append(field)

    schema = T.StructType(fields)
    return schema
