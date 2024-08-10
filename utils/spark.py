import inspect
from pyspark.sql import types as T

def to_pyspark_type(py_type: type) -> T.DataType:

    # explicit equality comparison, unlike in match-case
    if py_type == bool: return T.BooleanType
    elif py_type == int: return T.IntegerType
    elif py_type == float: return T.FloatType
    elif py_type == str: return T.StringType
    elif py_type == list: return T.ArrayType 
    else: raise TypeError('unable to map the type')


def annotations_to_schema(dataclass) -> T.StructType:

    annotations = inspect.get_annotations(dataclass)
    schema = T.StructType([
        T.StructField(name, to_pyspark_type(type_)())
        for name, type_ in annotations.items()
    ])
    return schema
