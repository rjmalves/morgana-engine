import sqlparse
from sqlparse.sql import Token
from sqlparse.sql import (
    Identifier,
    IdentifierList,
    Parenthesis,
    Comparison,
)
from sqlparse.tokens import Newline, Whitespace, Punctuation


def __filter_punctuation_tokens(tokens: list[Token]) -> list[Token]:
    __ETC_TOKENS = [Newline, Whitespace]
    return [t for t in tokens if t.ttype not in __ETC_TOKENS]


def query2tokens(query: str) -> list[Token]:
    statements = sqlparse.parse(query)
    return __filter_punctuation_tokens(statements[0].tokens)


def identifierlist2dict(
    identifier_list: IdentifierList,
) -> dict[str, list[str]]:
    tokens: list[Identifier] = [
        t for t in identifier_list.tokens if type(t) == Identifier
    ]
    parents: list[str] = list(set([t.get_parent_name() for t in tokens]))
    identifier_dict: dict[str, list[str]] = {a: [] for a in parents}
    for t in tokens:
        identifier_dict[t.get_parent_name()].append(t.get_real_name())
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
    parent = token.get_parent_name()
    if parent:
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
    table_alias_map: dict[str, str] = {},
    preprocessing_func=__filter_punctuation_tokens,
) -> dict[str, dict[str, str]]:
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
            print(tokens)
            return " ".join(__parse_filters(t) for t in token_or_list)
        elif ttype == Identifier:
            return __column_name_with_alias(token_or_list, table_alias_map)
        else:
            value = token_or_list.normalized
            return logical_operator_mappings.get(value, value)

    pandas_query_elements = []
    for t in preprocessing_func(tokens[1:]):
        pandas_query_elements.append(__parse_filters(t))
    return " ".join(pandas_query_elements)


def where2pandas(
    tokens: list[Token],
    table_alias_map: dict[str, str] = {},
    preprocessing_func=__filter_punctuation_tokens,
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
        if ttype in [Parenthesis, Identifier, Comparison]:
            return __parse_filters(preprocessing_func(token_or_list.tokens))
        elif ttype == list:
            return " ".join(__parse_filters(t) for t in token_or_list)
        elif ttype == Identifier:
            return __column_name_with_alias(token_or_list, table_alias_map)
        else:
            value = token_or_list.normalized
            return logical_operator_mappings.get(value, value)

    pandas_query_elements = []
    for t in preprocessing_func(tokens[1:]):
        pandas_query_elements.append(__parse_filters(t))
    return " ".join(pandas_query_elements)
