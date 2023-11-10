from abc import ABC
from app.adapters.repository.connection import Connection
from app.adapters.repository.dataio import factory as io_factory
from app.models.readingfilter import type_factory as readingfilter_factory
import pandas as pd
from sqlparse.sql import (  # type: ignore
    Token,
    Parenthesis,
    Comparison,
    Where,
    Identifier,
    IdentifierList,
)
from typing import Any
from os.path import join
from app.models.readingfilter import ReadingFilter
from app.utils.types import casting_functions
from app.utils.sql import partitions_in_file, partition_value_in_file
from app.utils.sql import (
    column_name_with_alias,
    filter_spacing_tokens,
    filter_punctuation_tokens,
    filter_spacing_and_punctuation_tokens,
    split_token_list_in_and_or_keywords,
)


class Processing(ABC):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        """
        Processes the tokens associated with the statement in the
        database specified in the connection.
        """
        raise NotImplementedError


class CREATE(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        raise NotImplementedError


class ALTER(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        raise NotImplementedError


class DROP(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        raise NotImplementedError


class INSERT(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        raise NotImplementedError


class UPDATE(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        raise NotImplementedError


class DELETE(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        raise NotImplementedError


class SELECT(Processing):
    """
    Processes a SELECT statement, for reading data.

    TODO - validate and handle errors
    TODO - support wildcard * in identifier list
    TODO - support joins
    TODO - support joins without aliases
    TODO - support GROUP BY
    TODO - support ORDER BY
    TODO - suport COUNT() function
    TODO - support LIMIT
    TODO - support OFFSET
    """

    @classmethod
    def __process_tables_identifiers(
        cls, tokens: list[Token]
    ) -> dict[str, str]:
        """
        Processes the tokens associated with the tables identifiers in
        order to obtain a map of alias: table_name.
        """
        # Iterate on tokens and find the table identifiers
        identifier_list = [t for t in tokens if type(t) is Identifier]
        alias_map: dict[str, str] = {
            t.get_alias(): t.get_real_name() for t in identifier_list
        }
        return alias_map

    @classmethod
    def __process_column_identifiers(
        cls, tokens: list[Token], tables_to_select: dict[str, str]
    ) -> dict[str, dict[str, str]]:
        """
        Processes the tokens associated with the column identifiers in
        order to obtain a map of table_alias: {column_alias: column_name}.
        """
        identifier_list = [t for t in tokens if type(t) is IdentifierList][0]
        identifier_tokens: list[Identifier] = [
            t for t in identifier_list.tokens if type(t) is Identifier
        ]
        parents: list[str] = list(
            set([t.get_parent_name() for t in identifier_tokens])
        )
        identifier_dict: dict[str, dict[str, str]] = {a: {} for a in parents}
        for t in identifier_tokens:
            identifier_dict[t.get_parent_name()][
                t.get_real_name()
            ] = column_name_with_alias(t, tables_to_select)
        return identifier_dict

    @classmethod
    def __process_join_mappings(
        cls, tokens: list[Token], tables_to_select: dict[str, str]
    ) -> dict[str, dict[str, str]]:
        """
        Processes the tokens associated with the join mappings in order
        to obtain a map of each join type with the tables that must be joined.
        """
        comparison_tokens = [t for t in tokens if type(t) is Comparison]
        mappings: list[tuple[str, str]] = []
        for c in comparison_tokens:
            identifiers = [
                t
                for t in filter_punctuation_tokens(c.tokens)
                if type(t) is Identifier
            ]
            mappings.append(
                tuple(
                    [
                        column_name_with_alias(i, tables_to_select)
                        for i in identifiers
                    ]
                )
            )
        return mappings

    @classmethod
    def __process_filters_for_reading(
        cls, tokens: list[Token]
    ) -> list[dict[str, ReadingFilter]]:
        """
        Filters the tokens after the WHERE that might help deciding which
        partition files must be read, optimizing reading time.

        Filters that are considered:

        - Column equality to constant
        - Column unequality to constant
        - Column belonging to set
        - Column not belonging to set

        """

        def extracts_reading_filters(token_or_list: Token | list):
            """ """
            ttype = type(token_or_list)
            if ttype == Parenthesis:
                splitted_list = split_token_list_in_and_or_keywords(
                    filter_spacing_tokens(token_or_list.tokens)
                )
                for token_list in splitted_list:
                    extracts_reading_filters(token_list)
            elif ttype == Comparison:
                # First case of interest:
                # Equality and Unequality
                comparison_tokens = filter_spacing_tokens(token_or_list.tokens)
                identifiers = [
                    t for t in comparison_tokens if type(t) is Identifier
                ]
                filter_type = readingfilter_factory(comparison_tokens)
                if filter_type:
                    filtermaps.append(
                        (
                            identifiers[0].get_parent_name(),
                            filter_type(comparison_tokens),
                        )
                    )
            elif ttype == list:
                # First case of interest:
                # Belonging and Not Belonging to Set
                comparison_tokens = filter_spacing_tokens(token_or_list)
                if len(comparison_tokens) == 1:
                    extracts_reading_filters(comparison_tokens[0])
                elif len(comparison_tokens) in [3, 4]:
                    identifiers = [
                        t for t in comparison_tokens if type(t) is Identifier
                    ]
                    filter_type = readingfilter_factory(comparison_tokens)
                    if filter_type:
                        filtermaps.append(
                            (
                                identifiers[0].get_parent_name(),
                                filter_type(comparison_tokens),
                            )
                        )

        where_tokens = [t for t in tokens if type(t) is Where]
        if len(where_tokens) > 0:
            splitted_tokens = split_token_list_in_and_or_keywords(
                filter_spacing_and_punctuation_tokens(tokens[1:])
            )
        filtermaps: list[dict[str, list[Token]]] = []
        for token_set in splitted_tokens:
            t_map = extracts_reading_filters(token_set)
            if not t_map:
                continue
            if type(t_map) is list:
                filtermaps += t_map
            else:
                filtermaps.append(t_map)
        else:
            return []

    @classmethod
    def __process_filters_for_querying(
        cls, tokens: list[Token], tables_to_select: dict[str, str]
    ):
        def process_filters_to_pandas_query(
            token_or_list: Token | list,
        ) -> str:
            ttype = type(token_or_list)
            logical_operator_mappings: dict[str, str] = {
                "=": "==",
                "AND": "&",
                "OR": "|",
                "NOT": "not",
                "IN": "in",
            }
            if ttype in [Parenthesis, IdentifierList, Comparison]:
                return process_filters_to_pandas_query(
                    filter_spacing_tokens(token_or_list.tokens)
                )
            elif ttype == list:
                return " ".join(
                    process_filters_to_pandas_query(t) for t in token_or_list
                )
            elif ttype == Identifier:
                return column_name_with_alias(token_or_list, tables_to_select)
            else:
                value = token_or_list.normalized
                return logical_operator_mappings.get(value, value)

        filters = [t for t in tokens if type(t) is Where]
        if len(filters) > 0:
            pandas_query_elements = []
            for t in filter_spacing_tokens(filters[0].tokens[1:]):
                pandas_query_elements.append(
                    process_filters_to_pandas_query(t)
                )
            return " ".join(pandas_query_elements)
        else:
            return None

    @classmethod
    def __process_select_from_table(
        cls,
        table: str,
        filters: list[ReadingFilter],
        columns: dict[str, str],
        conn: Connection,
    ):
        table_conn = conn.access(table)
        table_io = io_factory(table_conn.schema.format)

        # List partitioned columns from schema
        partition_columns: dict[str, str] = table_conn.schema.partition_keys

        # The main result is the list of filenames that must be read
        # and concatenated.
        if len(partition_columns) == 0:
            files_to_read: list[str] = [table]
        else:
            files_to_read: list[str] = []
            for c, c_type in partition_columns.items():
                partition_files = table_conn.list_partition_files(c)
                casting_func = casting_functions(c_type)
                # Builds a value: [files] map
                partition_maps: dict[Any, list[str]] = {}
                for f in partition_files:
                    v = casting_func(partition_value_in_file(f, c))
                    if v in partition_maps:
                        partition_maps[v].append(f)
                    else:
                        partition_maps[v] = [f]
                # List all possible values
                partition_values = [
                    casting_func(v) for v in list(partition_maps.keys())
                ]
                # Apply filters
                column_filters = [f for f in filters if f.column == c]
                if len(column_filters) == 0:
                    filtered_values = partition_values
                else:
                    filtered_values = []
                    for f in column_filters:
                        filtered_values += f.apply(
                            partition_values, casting_func
                        )
                    filtered_values = list(set(filtered_values))
                # Find files with values
                value_files = []
                for v in filtered_values:
                    value_files += partition_maps[v]
                files_to_read += value_files
            files_to_read = list(set(files_to_read))

        dfs: list[pd.DataFrame] = []
        for f in files_to_read:
            dff = table_io.read(
                join(table_conn.uri, f),
                storage_options=table_conn.storage_options,
            )
            # Adds partition values as columns
            f_partitions = partitions_in_file(f)
            for k, v in f_partitions.items():
                casting_func = casting_functions(
                    table_conn.schema.partition_keys[k]
                )
                if k in columns.keys():
                    dff[k] = casting_func(v)
            dfs.append(dff[columns.keys()].copy())
        df = pd.concat(dfs, ignore_index=True)
        # Rename due columns
        df.rename(
            columns=columns,
            inplace=True,
        )
        return df

    @classmethod
    def __process_select(
        cls,
        tables_to_select: dict[str, str],
        reading_filters: list[tuple[str, ReadingFilter]],
        columns_in_each_table: dict[str, dict[str, str]],
        conn: Connection,
    ) -> dict[str, pd.DataFrame]:
        """
        Processes the SELECT statement for each table separately,
        using the reading_filters for optimizing the file reading steps.
        """
        dfs: dict[str, pd.DataFrame] = {}
        for alias, name in tables_to_select.items():
            dfs[alias] = cls.__process_select_from_table(
                name,
                [f[1] for f in reading_filters if f[0] == alias],
                columns_in_each_table.get(alias, {}),
                conn,
            )
        return dfs

    @classmethod
    def __process_join_tables(
        cls,
        dfs: dict[str, pd.DataFrame],
        tables_to_select: dict[str, str],
        table_join_mappings: dict[str, dict[str, str]],
    ) -> pd.DataFrame:
        """
        Processes the JOIN keywords in the query, joining the tables in the order
        they appear in the statement.
        """
        if len(table_join_mappings) > 0:
            return None
        else:
            return dfs[list(tables_to_select.keys())[0]]

    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict | None:
        tables_to_select = cls.__process_tables_identifiers(tokens)
        columns_in_each_table = cls.__process_column_identifiers(
            tokens, tables_to_select
        )
        table_join_mappings = cls.__process_join_mappings(
            tokens, tables_to_select
        )
        reading_filters = cls.__process_filters_for_reading(tokens)
        querying_filters = cls.__process_filters_for_querying(
            tokens, tables_to_select
        )
        dfs = cls.__process_select(
            tables_to_select, reading_filters, columns_in_each_table, conn
        )
        df = cls.__process_join_tables(
            dfs, tables_to_select, table_join_mappings
        )
        if querying_filters is not None:
            df = df.query(querying_filters)
        return df


MAPPING: dict[str, type[Processing]] = {
    "CREATE": CREATE,
    "ALTER": ALTER,
    "DROP": DROP,
    "INSERT": INSERT,
    "UPDATE": UPDATE,
    "DELETE": DELETE,
    "SELECT": SELECT,
}


def factory(kind: str) -> type[Processing]:
    return MAPPING[kind]
