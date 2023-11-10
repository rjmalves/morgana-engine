from abc import ABC
from sqlparse.sql import Token
from sqlparse.sql import (
    Identifier,
    Parenthesis,
)
from typing import TypeVar, Callable, override
from sqlparse.tokens import Newline, Whitespace, Punctuation

T = TypeVar("T")


def _filter_space_and_punctuation_tokens(tokens: list[Token]) -> list[Token]:
    __ETC_TOKENS = [Newline, Whitespace, Punctuation]
    return [t for t in tokens if t.ttype not in __ETC_TOKENS]


class ReadingFilter(ABC):
    def __init__(self, tokens: list[Token]) -> None:
        super().__init__()
        self.tokens = tokens
        self._column: str | None = None
        self._operators: list[str] | None = None
        self._values: list[str] | None = None

    @property
    def column(self) -> str:
        if self._column is None:
            self._column = [t for t in self.tokens if type(t) == Identifier][
                0
            ].get_real_name()
        return self._column

    @property
    def operators(self) -> list[str]:
        raise NotImplementedError

    @property
    def values(self) -> list[str]:
        raise NotImplementedError

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        raise NotImplementedError

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        raise NotImplementedError


class EqualityReadingFilter(ReadingFilter):
    @override
    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                [t for t in self.tokens if "Comparison" in str(t.ttype)][
                    0
                ].value
            ]
        return self._operators

    @override
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
        identifiers = [t for t in tokens if type(t) == Identifier]
        not_identifiers = [t for t in tokens if type(t) != Identifier]
        num_identifiers = len(identifiers)
        ttypes = [t.ttype for t in not_identifiers if t.ttype is not None]
        comparisons = [t for t in ttypes if "Comparison" in str(t)]
        num_comparison = len(comparisons)
        num_constants = len([t for t in ttypes if "Token.Literal" in str(t)])
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
    def __revert_operator(self, operator: str) -> str:
        operator_map: dict[str, str] = {
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
        }
        return operator_map[operator]

    @override
    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                [t for t in self.tokens if "Comparison" in str(t.ttype)][
                    0
                ].value
            ]
            identifier_first = type(self.tokens[0]) == Identifier
            if not identifier_first:
                self._operators = [self.__revert_operator(self.operators[0])]
        return self._operators

    @override
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
        identifiers = [t for t in tokens if type(t) == Identifier]
        not_identifiers = [t for t in tokens if type(t) != Identifier]
        num_identifiers = len(identifiers)
        ttypes = [t.ttype for t in not_identifiers if t.ttype is not None]
        comparisons = [t for t in ttypes if "Comparison" in str(t)]
        num_comparison = len(comparisons)
        num_constants = len([t for t in ttypes if "Token.Literal" in str(t)])
        if all(
            [n == 1 for n in [num_identifiers, num_comparison, num_constants]]
        ):
            if comparisons[0].value in [">", "<", ">=", "<="]:
                return True
        return False

    def apply(self, values: list[T], casting_func: Callable) -> list[T]:
        casted_values = self._values = [casting_func(v) for v in self.values]
        if self.operators[0] == ">":
            return [v for v in values if v > casted_values[0]]
        elif self.operators[0] == "<":
            return [v for v in values if v < casted_values[0]]
        elif self.operators[0] == ">=":
            return [v for v in values if v >= casted_values[0]]
        elif self.operators[0] == "<=":
            return [v for v in values if v <= casted_values[0]]


class InSetReadingFilter(ReadingFilter):
    @override
    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                t for t in self.tokens if type(t) == Identifier
            ][0].get_real_name()
        return self._operators

    @override
    @property
    def values(self) -> list[str]:
        if self._values is None:
            collection = _filter_space_and_punctuation_tokens(
                [t for t in self.tokens if type(t) == Parenthesis][0].tokens
            )
            self._values = collection[0].value.split(",")
        return self._values

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        identifiers = [t for t in tokens if type(t) == Identifier]
        num_identifiers = len(identifiers)
        num_in_keywords = len([t for t in tokens if t.normalized == "IN"])
        num_not_keywords = len([t for t in tokens if t.normalized == "NOT"])
        num_parenthesis = len([t for t in tokens if type(t) == Parenthesis])
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
    @override
    @property
    def operators(self) -> list[str]:
        if self._operators is None:
            self._operators = [
                t for t in self.tokens if type(t) == Identifier
            ][0].get_real_name()
        return self._operators

    @override
    @property
    def values(self) -> list[str]:
        if self._values is None:
            collection = _filter_space_and_punctuation_tokens(
                [t for t in self.tokens if type(t) == Parenthesis][0].tokens
            )
            self._values = collection[0].value.split(",")
        return self._values

    @classmethod
    def is_filter(cls, tokens: list[Token]) -> bool:
        identifiers = [t for t in tokens if type(t) == Identifier]
        num_identifiers = len(identifiers)
        num_in_keywords = len([t for t in tokens if t.normalized == "IN"])
        num_not_keywords = len([t for t in tokens if t.normalized == "NOT"])
        num_parenthesis = len([t for t in tokens if type(t) == Parenthesis])
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
