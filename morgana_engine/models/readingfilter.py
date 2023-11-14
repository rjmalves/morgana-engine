from abc import ABC
from typing import TypeVar, Callable
from sqlparse.sql import Token, Identifier, Parenthesis  # type: ignore
from morgana_engine.utils.sql import filter_spacing_and_punctuation_tokens

T = TypeVar("T")


class ReadingFilter(ABC):
    """
    Class that defines a filter that is applied when reading
    partitioned files in a database. The filter aims to reduce the
    number of files that are processed by the database engine,
    reducing the query time.

    Attributes:
    -----------
    tokens : list[Token]
        A list of tokens that represent the filter expression.
    column : str | None
        The name of the column that the filter is applied to.
    operators : list[str] | None
        A list of operators used in the filter expression.
    values : list[str] | None
        A list of values used in the filter expression.
    """

    def __init__(self, tokens: list[Token]) -> None:
        super().__init__()
        self.tokens = tokens
        self._column: str | None = None
        self._operators: list[str] | None = None
        self._values: list[str] | None = None

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        else:
            return all(
                [
                    self.column == o.column,
                    self.operators == o.operators,
                    self.values == o.values,
                ]
            )

    @property
    def column(self) -> str:
        """
        Returns the name of the column that the filter is applied to.

        Returns:
        --------
        str
            The name of the column.
        """
        if self._column is None:
            self._column = [t for t in self.tokens if type(t) is Identifier][
                0
            ].get_real_name()
        return self._column

    @property
    def operators(self) -> list[str]:
        """
        Returns a list of operators used in the filter expression.

        Returns:
        --------
        list[str]
            A list of operators.
        """
        raise NotImplementedError

    @property
    def values(self) -> list[str]:
        """
        Returns a list of values used in the filter expression.

        Returns:
        --------
        list[str]
            A list of values.
        """
        raise NotImplementedError

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        """
        Determines whether the given list of tokens represents a filter.

        Parameters:
        -----------
        tokens : list[Token]
            A list of tokens.

        Returns:
        --------
        bool
            True if the list of tokens represents a filter, False otherwise.
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

    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                [t for t in self.tokens if "Comparison" in str(t.ttype)][
                    0
                ].value
            ]
        return self._operators

    @property
    def values(self) -> list[str]:
        if self._values is None:
            self._values = [
                [t for t in self.tokens if "Token.Literal" in str(t.ttype)][
                    0
                ].value
            ]
        return self._values

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        identifiers = [t for t in tokens if type(t) is Identifier]
        not_identifiers = [t for t in tokens if type(t) is not Identifier]
        num_identifiers = len(identifiers)
        comparisons = [
            t for t in not_identifiers if "Comparison" in str(t.ttype)
        ]
        num_comparison = len(comparisons)
        num_constants = len(
            [t for t in not_identifiers if "Token.Literal" in str(t.ttype)]
        )
        if all(
            [n == 1 for n in [num_identifiers, num_comparison, num_constants]]
        ):
            if comparisons[0].value in ["=", "!="]:
                return True
        return False

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        if self.operators[0] == "=":
            return [v for v in values if v in casted_values]
        else:
            return [v for v in values if v not in casted_values]


class UnequalityReadingFilter(ReadingFilter):
    """
    Filter that covers the case where a column is compared to a constant
    with the difference operator.
    """

    def __revert_operator(self, operator: str) -> str:
        operator_map: dict[str, str] = {
            ">": "<",
            "<": ">",
            ">=": "<=",
            "<=": ">=",
        }
        return operator_map[operator]

    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                [t for t in self.tokens if "Comparison" in str(t.ttype)][
                    0
                ].value
            ]
            identifier_first = type(self.tokens[0]) is Identifier
            if not identifier_first:
                self._operators = [self.__revert_operator(self.operators[0])]
        return self._operators

    @property
    def values(self) -> list[str]:
        if self._values is None:
            self._values = [
                [t for t in self.tokens if "Token.Literal" in str(t.ttype)][
                    0
                ].value
            ]
        return self._values

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        identifiers = [t for t in tokens if type(t) is Identifier]
        not_identifiers = [t for t in tokens if type(t) is not Identifier]
        num_identifiers = len(identifiers)
        comparisons = [
            t for t in not_identifiers if "Comparison" in str(t.ttype)
        ]
        num_comparison = len(comparisons)
        num_constants = len(
            [t for t in not_identifiers if "Token.Literal" in str(t.ttype)]
        )
        if all(
            [n == 1 for n in [num_identifiers, num_comparison, num_constants]]
        ):
            if comparisons[0].value in [">", "<", ">=", "<="]:
                return True
        return False

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = [casting_func(v) for v in self.values]
        if self.operators[0] == ">":
            return [v for v in values if v > casted_values[0]]
        elif self.operators[0] == "<":
            return [v for v in values if v < casted_values[0]]
        elif self.operators[0] == ">=":
            return [v for v in values if v >= casted_values[0]]
        elif self.operators[0] == "<=":
            return [v for v in values if v <= casted_values[0]]
        return []


class InSetReadingFilter(ReadingFilter):
    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                t.normalized
                for t in self.tokens
                if t.normalized in ["IN", "NOT"]
            ]
        return self._operators

    @property
    def values(self) -> list[str]:
        if self._values is None:
            collection = filter_spacing_and_punctuation_tokens(
                [t for t in self.tokens if type(t) is Parenthesis][0].tokens
            )
            self._values = collection[0].value.split(",")
            self._values = [v.strip() for v in self._values]
        return self._values

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        identifiers = [t for t in tokens if type(t) is Identifier]
        num_identifiers = len(identifiers)
        num_in_keywords = len([t for t in tokens if t.normalized == "IN"])
        num_not_keywords = len([t for t in tokens if t.normalized == "NOT"])
        num_parenthesis = len([t for t in tokens if type(t) is Parenthesis])
        return all(
            [
                n == 1
                for n in [
                    num_identifiers,
                    num_in_keywords,
                    num_parenthesis,
                ]
            ]
            + [num_not_keywords == 0]
        )

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        return [v for v in values if v in casted_values]


class NotInSetReadingFilter(ReadingFilter):
    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                t.normalized
                for t in self.tokens
                if t.normalized in ["IN", "NOT"]
            ]
        return self._operators

    @property
    def values(self) -> list[str]:
        if self._values is None:
            collection = filter_spacing_and_punctuation_tokens(
                [t for t in self.tokens if type(t) is Parenthesis][0].tokens
            )
            self._values = collection[0].value.split(",")
            self._values = [v.strip() for v in self._values]

        return self._values

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        identifiers = [t for t in tokens if type(t) is Identifier]
        num_identifiers = len(identifiers)
        num_in_keywords = len([t for t in tokens if t.normalized == "IN"])
        num_not_keywords = len([t for t in tokens if t.normalized == "NOT"])
        num_parenthesis = len([t for t in tokens if type(t) is Parenthesis])
        return all(
            [
                n == 1
                for n in [
                    num_identifiers,
                    num_in_keywords,
                    num_parenthesis,
                ]
            ]
            + [num_not_keywords == 1]
        )

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        return [v for v in values if v not in casted_values]


def type_factory(token_list: list[Token]) -> type[ReadingFilter] | None:
    for t in [
        EqualityReadingFilter,
        UnequalityReadingFilter,
        InSetReadingFilter,
        NotInSetReadingFilter,
    ]:
        if t.is_filter(token_list):
            return t
    return None
