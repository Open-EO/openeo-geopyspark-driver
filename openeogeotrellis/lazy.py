from typing import Callable, TypeVar, Generic

T = TypeVar('T')


class Lazy(Generic[T]):
    _UNSET = object()

    def __init__(self, get_value: Callable[[], T]):
        self._get_value = get_value
        self._value = self._UNSET

    @property
    def value(self) -> T:
        if self._value == self._UNSET:
            self._value = self._get_value()

        return self._value
