import sqlparse  # type: ignore
from sqlparse.sql import Token, Identifier  # type: ignore
from sqlparse.tokens import Newline, Whitespace, Punctuation  # type: ignore
from typing import TypeVar

T = TypeVar("T")


def __filter_tokens(tokens: list[Token], ttypes: list[T]) -> list[Token]:
    return [t for t in tokens if t.ttype not in ttypes]


def filter_punctuation_tokens(tokens: list[Token]) -> list[Token]:
    tokens_to_ignore = [Punctuation]
    return __filter_tokens(tokens, tokens_to_ignore)


def filter_spacing_tokens(tokens: list[Token]) -> list[Token]:
    tokens_to_ignore = [Newline, Whitespace]
    return __filter_tokens(tokens, tokens_to_ignore)


def filter_spacing_and_punctuation_tokens(tokens: list[Token]) -> list[Token]:
    tokens_to_ignore = [Newline, Whitespace, Punctuation]
    return __filter_tokens(tokens, tokens_to_ignore)


def query2tokens(query: str) -> list[Token]:
    statements = sqlparse.parse(query)
    return filter_spacing_tokens(statements[0].tokens)


def column_name_with_alias(
    token: Identifier, table_alias_map: dict[str, str]
) -> str:
    alias = token.get_alias()
    parent = token.get_parent_name()
    if alias:
        return alias
    elif parent:
        return f"{table_alias_map.get(parent, parent)}_{token.get_real_name()}"
    else:
        return token.get_real_name()


def split_token_list_in_and_or_keywords(
    token_list: list[Token],
) -> list[list[Token]]:
    """
    Splits a list of tokens into sublists based on the occurrence of
    "AND" and "OR" keywords.

    Args:
        token_list (list[Token]): The list of tokens to split.

    Returns:
        list[list[Token]]: A list of sublists, where each sublist contains
        tokens separated by "AND" or "OR" keywords.
    """
    ands = [t for t in token_list if t.normalized == "AND"]
    ors = [t for t in token_list if t.normalized == "OR"]
    and_indices = [token_list.index(t) for t in ands]
    or_indices = [token_list.index(t) for t in ors]
    and_or_indices = sorted(and_indices + or_indices)
    if len(and_or_indices) > 0:
        split_indices = [-1] + and_or_indices + [len(token_list)]
        return [
            token_list[begin_index + 1 : end_index]
            for begin_index, end_index in zip(
                split_indices[:-1], split_indices[1:]
            )
        ]
    else:
        return [token_list]


def partitions_in_file(filename: str) -> dict[str, str]:
    parts = filename.split("-")[1:-1]
    partition_values: dict[str, str] = {}
    for p in parts:
        part_key_value = p.split("=")
        partition_values[part_key_value[0]] = part_key_value[1]
    return partition_values


def partition_value_in_file(filename: str, column: str) -> str | None:
    return partitions_in_file(filename).get(column)
