from abc import ABC
from typing import TypeVar, Callable
from morgana_engine.models.sql import SQLToken, SQLTokenType
from morgana_engine.models.parsedsql import Column


T = TypeVar("T")


class ReadingFilter(ABC):
    """
    Class that defines a filter that is applied when reading
    partitioned files in a database. The filter aims to reduce the
    number of files that are processed by the database engine,
    reducing the query time.

    Attributes:
    -----------
    column : Column
        The column that the filter is applied to.
    operators : list[str] | None
        A list of operators used in the filter expression.
    values : list[str] | None
        A list of values used in the filter expression.
    """

    def __init__(
        self, column: Column, operator: SQLToken, values: list[SQLToken]
    ) -> None:
        super().__init__()
        self._column: Column = column
        self._operator: SQLToken = operator
        self._values: list[SQLToken] = values

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        else:
            return all(
                [
                    self.column == o.column,
                    self.operator == o.operator,
                    self.values == o.values,
                ]
            )

    @property
    def column(self) -> Column:
        """
        Returns the Column object that the filter is applied to.

        Returns:
        --------
        Column
            The column object.
        """
        return self._column

    @property
    def operator(self) -> SQLToken:
        """
        Returns the operator used in the filter expression.

        Returns:
        --------
        str
            The operator.
        """
        return self._operator

    @property
    def values(self) -> list[str]:
        """
        Returns a list of values used in the filter expression.

        Returns:
        --------
        list[str]
            A list of values.
        """
        return [t.text for t in self._values]

    @classmethod
    def is_filter(cls, operation: SQLToken) -> bool:
        """
        Determines whether the given token represents a filter.

        Parameters:
        -----------
        operation : SQLToken
            The token that describes the operation.

        Returns:
        --------
        bool
            True if the token represents a filter, False otherwise.
        """
        raise NotImplementedError

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        """
        Applies the filter to the given list of values.

        Parameters:
        -----------
        values : list[T]
            A list of values to be filtered.
        casting_func : Callable
            A function used to cast the values to the appropriate type.

        Returns:
        --------
        list[T]
            A list of filtered values.
        """
        raise NotImplementedError


class EqualityReadingFilter(ReadingFilter):
    """
    Filter that covers the case where a column is compared to a constant
    with a equality operator.
    """

    @classmethod
    def is_filter(cls, token: SQLToken) -> bool:
        return token.type == SQLTokenType.EQUALS

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        if self.operator.type == SQLTokenType.EQUALS:
            return [v for v in values if v in casted_values]
        else:
            return [v for v in values if v not in casted_values]


class UnequalityReadingFilter(ReadingFilter):
    """
    Filter that covers the case where a column is compared to a constant
    with the difference operator.
    """

    @classmethod
    def is_filter(cls, token: SQLToken) -> bool:
        return token.type in [
            SQLTokenType.GREATER,
            SQLTokenType.GREATER_EQUAL,
            SQLTokenType.LESS,
            SQLTokenType.LESS_EQUAL,
        ]

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = [casting_func(v) for v in self.values]
        if self.operator.type == SQLTokenType.GREATER:
            return [v for v in values if v > casted_values[0]]
        elif self.operator.type == SQLTokenType.LESS:
            return [v for v in values if v < casted_values[0]]
        elif self.operator.type == SQLTokenType.GREATER_EQUAL:
            return [v for v in values if v >= casted_values[0]]
        elif self.operator.type == SQLTokenType.LESS_EQUAL:
            return [v for v in values if v <= casted_values[0]]
        return []


class InSetReadingFilter(ReadingFilter):

    @classmethod
    def is_filter(cls, token: SQLToken) -> bool:
        return token.type == SQLTokenType.IN

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        return [v for v in values if v in casted_values]


class NotInSetReadingFilter(ReadingFilter):

    @classmethod
    def is_filter(cls, token: SQLToken) -> bool:
        return token.type == SQLTokenType.NOT_IN

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        return [v for v in values if v not in casted_values]


def type_factory(operation_token: SQLToken) -> type[ReadingFilter] | None:
    for t in [
        EqualityReadingFilter,
        UnequalityReadingFilter,
        InSetReadingFilter,
        NotInSetReadingFilter,
    ]:
        if t.is_filter(operation_token):
            return t
    return None
