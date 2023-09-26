from typing import Any, Iterable


def is_sub_iterable(small: Iterable[Any], big: Iterable[Any]) -> bool:
    return all(elem in big for elem in small)
