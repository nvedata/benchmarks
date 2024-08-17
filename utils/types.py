from collections.abc import Iterable, Hashable


def to_hashable(obj: object) -> Hashable:

    if isinstance(obj, Hashable):
        return obj
    else:
        if isinstance(obj, Iterable):
            return tuple(to_hashable(i) for i in obj)
        else:
            raise NotImplementedError(
                f'Type {type(obj)} does not have '
                '`__hash__` method and non-iterable'
            )
