from pyspark.sql.types import DataType, StringType, FloatType, IntegerType

def to_pyspark_type(py_type: type) -> DataType:
    # explicit equality comparison, unlike in match-case
    if py_type == int: return IntegerType
    elif py_type == float: return FloatType
    elif py_type == str: return StringType
    else: raise TypeError('unable to map the type')
