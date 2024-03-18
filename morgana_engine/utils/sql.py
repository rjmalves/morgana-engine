import sqlparse  # type: ignore
from morgana_engine.models.parsedsql import Column, Table
from sqlparse.sql import Token, Identifier, Parenthesis, Comparison  # type: ignore
from sqlparse.tokens import Newline, Whitespace, Punctuation  # type: ignore
from typing import TypeVar, Type, List

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


def _check_types(tokens: list[Token], types: list[Type[Token]]) -> bool:
    for i, token in enumerate(tokens):
        if not isinstance(token, types[i]):
            return False
    return True


def _match_token_types(
    tokens: list[Token], types: list[Type[Token]]
) -> List[List[int]]:
    n_types = len(types)
    grouping_indices = []
    for i in range(len(tokens) - (n_types - 1)):
        if _check_types(tokens[i : i + n_types], types):
            grouping_indices.append(list(range(i, n_types)))
    return grouping_indices


def group_set_tokens_parentheses(tokens: list[Token]) -> list[list[Token]]:
    """
    Groups tokens that define IN or NOT IN operations in a single
    comparison token.

    Args:
        tokens (list[Token]): The list of tokens to group.

    Returns:
        list[Token]: A list of grouped tokens, when the tokens are
        separated by IN or NOT IN operations.
    """
    in_token_types = [Identifier, Token, Parenthesis]
    n_in_tokens = len(in_token_types)
    not_in_token_types = [Identifier, Token, Token, Parenthesis]
    n_not_in_tokens = len(not_in_token_types)

    in_grouping_indices = _match_token_types(tokens, in_token_types)
    not_in_grouping_indices = _match_token_types(tokens, not_in_token_types)
    # Groups 'IN' tokens
    grouped_tokens = []
    for i, t in enumerate(tokens):
        found_in_group = False
        for in_group in in_grouping_indices:
            if i in in_group:
                found_in_group = True
                if i == in_group[0]:
                    grouped_tokens.append(
                        Comparison(tokens[i : i + n_in_tokens])
                    )
                break
        for not_in_group in not_in_grouping_indices:
            if i in not_in_group:
                found_in_group = True
                if i == not_in_group[0]:
                    grouped_tokens.append(
                        Comparison(tokens[i : i + n_not_in_tokens])
                    )
                break
        if not found_in_group:
            grouped_tokens.append(t)

    return grouped_tokens


def table_from_column_token(token: Identifier, tables: list[Table]) -> Table:
    """
    Extracts the table object associated with the token that refers to a
    column.
    """
    # Find the table associated with the token
    parent = token.get_parent_name()
    if parent:
        return list(filter(lambda t: t.alias == parent, tables))[0]
    name = token.get_real_name()
    for t in tables:
        for c in t.columns:
            if c.alias and c.alias == name:
                return t
        for c in t.columns:
            if c.name == name:
                return t

    raise ValueError(f"Table not found for token {token}")


def column_from_token(token: Identifier, tables: list[Table]) -> Column:
    """
    Extracts the column object associated with the token.
    """
    # Find the table associated with the token
    table = table_from_column_token(token, tables)
    # Find the column associated with the token
    alias = token.get_alias()
    name = token.get_real_name()
    if alias:
        column = list(filter(lambda c: c.alias == alias, table.columns))[0]
    else:
        # Needs to check if the token has a name, which is an alias
        # of any column.
        columns = list(filter(lambda c: c.name == name, table.columns))
        if len(columns) == 0:
            columns = list(filter(lambda c: c.alias == name, table.columns))
        column = columns[0]

    return column


def add_column_information_from_token(
    token: Identifier, table_alias_map: list[Table]
):
    alias = token.get_alias()
    parent = token.get_parent_name()
    name = token.get_real_name()
    table = list(filter(lambda t: t.alias == parent, table_alias_map))[0]
    table.columns.append(
        Column(
            name=name,
            alias=alias,
            type_str=None,
            table_name=table.name,
            table_alias=table.alias,
            has_parent_in_token=parent is not None,
            partition=False,
        )
    )


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
    parts = [p for p in filename.split("-")[1:] if len(p) > 0]
    partition_values: dict[str, str] = {}
    for p in parts:
        part_key_value = p.split("=")
        partition_values[part_key_value[0]] = part_key_value[1]
    return partition_values


def partition_value_in_file(filename: str, column: str) -> str | None:
    return partitions_in_file(filename).get(column)
