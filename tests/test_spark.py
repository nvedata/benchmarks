import pytest
from utils.spark import annotation_is_list

test_annotation_is_type_parameters = [
    (int, False),
    (list, True),
    (list[int], True),
    (list[int | float], True),
    (str, False),
    (list | None, True),
    (list[str] | None, True)
]

@pytest.mark.parametrize("annotation,expected", test_annotation_is_type_parameters)
def test_annotation_is_list(annotation, expected):
    assert annotation_is_list(annotation) == expected
