from collections.abc import Hashable
from utils.types import to_hashable


def test_to_hashable():
    nonhashable_obj = [
        1,
        'abc',
        [1, 'abc'],
        [1, 'abc', [1, 'abc', None]]
    ]
    assert not isinstance(nonhashable_obj, Hashable)
    hashable_obj = to_hashable(nonhashable_obj)
    assert isinstance(hashable_obj, Hashable)
