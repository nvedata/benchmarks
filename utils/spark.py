import inspect
from pyspark.sql.types import DataType, StringType, FloatType, IntegerType, StructType, StructField

def to_pyspark_type(py_type: type) -> DataType:
    
    # explicit equality comparison, unlike in match-case
    if py_type == int: return IntegerType
    elif py_type == float: return FloatType
    elif py_type == str: return StringType
    else: raise TypeError('unable to map the type')


def annotations_to_schema(dataclass) -> StructType:

    annotations = inspect.get_annotations(dataclass)
    schema = StructType([
        StructField(name, to_pyspark_type(type_)())
        for name, type_ in annotations.items()
    ])
    return schema
