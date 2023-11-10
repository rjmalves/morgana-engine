import sqlparse
from sqlparse.sql import Token
from sqlparse.sql import (
    Identifier,
    IdentifierList,
    Parenthesis,
    Comparison,
)
from app.models.readingfilter import type_factory
from typing import TypeVar
from sqlparse.tokens import Newline, Whitespace, Punctuation

T = TypeVar("T")


def __filter_punctuation_tokens(tokens: list[Token]) -> list[Token]:
    __ETC_TOKENS = [Punctuation]
    return [t for t in tokens if t.ttype not in __ETC_TOKENS]


def __filter_space_tokens(tokens: list[Token]) -> list[Token]:
    __ETC_TOKENS = [Newline, Whitespace]
    return [t for t in tokens if t.ttype not in __ETC_TOKENS]


def query2tokens(query: str) -> list[Token]:
    statements = sqlparse.parse(query)
    return __filter_space_tokens(statements[0].tokens)


def identifierlist2dict(
    identifier_list: IdentifierList, table_alias_map: dict[str, str]
) -> dict[str, dict[str, str]]:
    tokens: list[Identifier] = [
        t for t in identifier_list.tokens if type(t) == Identifier
    ]
    parents: list[str] = list(set([t.get_parent_name() for t in tokens]))
    identifier_dict: dict[str, list[str]] = {a: {} for a in parents}
    for t in tokens:
        identifier_dict[t.get_parent_name()][
            t.get_real_name()
        ] = __column_name_with_alias(t, table_alias_map)
    return identifier_dict


def aliases2dict(
    identifiers: list[Identifier],
) -> dict[str, str]:
    alias_map: dict[str, str] = {
        t.get_alias(): t.get_real_name() for t in identifiers
    }
    return alias_map


def __column_name_with_alias(
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


def join_comparison_mapping(
    comparisons: list[Comparison], table_alias_map: dict[str, str]
) -> list[tuple[str, str]]:
    mappings: list[tuple[str, str]] = []
    for c in comparisons:
        identifiers = [
            t
            for t in __filter_punctuation_tokens(c.tokens)
            if type(t) == Identifier
        ]
        mappings.append(
            tuple(
                [
                    __column_name_with_alias(i, table_alias_map)
                    for i in identifiers
                ]
            )
        )
    return mappings


def where2filtermap(
    tokens: list[Token],
    preprocessing_func=__filter_space_tokens,
) -> list[tuple[str, list[Token]]]:
    def __split_list_and_or(token_list: list[Token]) -> list[list[Token]]:
        # Breaks the list by AND / OR keywords
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

    def __parse_filters(token_or_list: Token | list):
        ttype = type(token_or_list)
        if ttype == Parenthesis:
            splitted_list = __split_list_and_or(
                preprocessing_func(token_or_list.tokens)
            )
            for token_list in splitted_list:
                __parse_filters(token_list)
        elif ttype == Comparison:
            # First case of interest:
            # - One Identifier
            # - Comparison
            # - One constant value (Integer, Float, Simple)
            comparison_tokens = preprocessing_func(token_or_list.tokens)
            identifiers = [
                t for t in comparison_tokens if type(t) == Identifier
            ]
            filter_type = type_factory(comparison_tokens)
            if filter_type:
                filtermaps.append(
                    (
                        identifiers[0].get_parent_name(),
                        filter_type(comparison_tokens),
                    )
                )
        elif ttype == list:
            comparison_tokens = preprocessing_func(token_or_list)
            if len(comparison_tokens) == 1:
                __parse_filters(comparison_tokens[0])
            elif len(comparison_tokens) in [3, 4]:
                # Second case of interest:
                # - One Identifier
                # - IN or NOT IN keywords
                # - Set of values
                identifiers = [
                    t for t in comparison_tokens if type(t) == Identifier
                ]
                filter_type = type_factory(comparison_tokens)
                if filter_type:
                    filtermaps.append(
                        (
                            identifiers[0].get_parent_name(),
                            filter_type(comparison_tokens),
                        )
                    )

    splitted_tokens = __split_list_and_or(
        __filter_punctuation_tokens(preprocessing_func(tokens[1:]))
    )
    filtermaps: list[dict[str, list[Token]]] = []
    for token_set in splitted_tokens:
        t_map = __parse_filters(token_set)
        if not t_map:
            continue
        if type(t_map) == list:
            filtermaps += t_map
        else:
            filtermaps.append(t_map)
    return filtermaps


def where2pandas(
    tokens: list[Token],
    table_alias_map: dict[str, str] = {},
    preprocessing_func=__filter_space_tokens,
) -> str:
    def __parse_filters(token_or_list: Token | list) -> str:
        ttype = type(token_or_list)
        logical_operator_mappings: dict[str, str] = {
            "=": "==",
            "AND": "&",
            "OR": "|",
            "NOT": "not",
            "IN": "in",
        }
        if ttype in [Parenthesis, IdentifierList, Comparison]:
            return __parse_filters(preprocessing_func(token_or_list.tokens))
        elif ttype == list:
            return " ".join(__parse_filters(t) for t in token_or_list)
        elif ttype == Identifier:
            return __column_name_with_alias(token_or_list, table_alias_map)
        else:
            value = token_or_list.normalized
            return logical_operator_mappings.get(value, value)

    pandas_query_elements = []
    for t in __filter_punctuation_tokens(preprocessing_func(tokens[1:])):
        pandas_query_elements.append(__parse_filters(t))
    return " ".join(pandas_query_elements)


def partitioned_file_name(table_name: str, partitions: dict[str, str]) -> str:
    ordered_keys = sorted(list(partitions.keys()))
    filename = f"{table_name}"
    for k in ordered_keys:
        filename += f"-{k}={ordered_keys[k]}"
    return filename + "-"


def partitions_in_file(filename: str) -> dict[str, str]:
    parts = filename.split("-")[1:-1]
    partition_values: dict[str, str] = {}
    for p in parts:
        part_key_value = p.split("=")
        partition_values[part_key_value[0]] = part_key_value[1]
    return partition_values


def partition_value_in_file(filename: str, column: str) -> str | None:
    return partitions_in_file(filename).get(column)
