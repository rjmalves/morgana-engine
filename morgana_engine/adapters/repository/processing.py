from abc import ABC
from morgana_engine.adapters.repository.connection import Connection
from morgana_engine.adapters.repository.dataio import factory as io_factory
from morgana_engine.models.readingfilter import (
    type_factory as readingfilter_factory,
)
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
from morgana_engine.models.readingfilter import ReadingFilter
from morgana_engine.utils.types import casting_functions
from morgana_engine.utils.sql import (
    partitions_in_file,
    partition_value_in_file,
)
from morgana_engine.utils.sql import (
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
    ) -> pd.DataFrame | dict:
        """
        Processes the tokens associated with the statement in the
        database specified in the connection.
        """
        raise NotImplementedError


class CREATE(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
        raise NotImplementedError


class ALTER(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
        raise NotImplementedError


class DROP(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
        raise NotImplementedError


class INSERT(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
        raise NotImplementedError


class UPDATE(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
        raise NotImplementedError


class DELETE(Processing):
    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
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
    def _process_table_identifiers(cls, tokens: list[Token]) -> dict[str, str]:
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
    def _process_column_identifiers(
        cls, tokens: list[Token], tables_to_select: dict[str, str]
    ) -> dict[str, dict[str, str]]:
        """
        Processes the tokens associated with the column identifiers in
        order to obtain a map of table_alias: {column_alias: column_name}.
        """
        # TODO - think about how to signal that a wildcard exists
        if "*" in [t.normalized for t in tokens]:
            return {a: {} for a in tables_to_select.keys()}
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
    def _process_join_mappings(
        cls, tokens: list[Token], tables_to_select: dict[str, str]
    ) -> list[tuple[str, str]]:
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
                (
                    column_name_with_alias(identifiers[0], tables_to_select),
                    column_name_with_alias(identifiers[1], tables_to_select),
                )
            )
        return mappings

    @classmethod
    def _extract_reading_filters(
        cls, token_or_list: Token | list[Token]
    ) -> list[tuple[str, ReadingFilter]]:
        if type(token_or_list) is Parenthesis:
            splitted_list = split_token_list_in_and_or_keywords(
                filter_spacing_tokens(token_or_list.tokens)
            )
            filters = []
            for token_list in splitted_list:
                filters += cls._extract_reading_filters(token_list)
            return filters
        elif type(token_or_list) is Comparison:
            # First case of interest:
            # Equality and Unequality
            comparison_tokens = filter_spacing_tokens(token_or_list.tokens)
            identifiers = [
                t for t in comparison_tokens if type(t) is Identifier
            ]
            filter_type = readingfilter_factory(comparison_tokens)
            if filter_type:
                return [
                    (
                        identifiers[0].get_parent_name(),
                        filter_type(comparison_tokens),
                    )
                ]
        elif type(token_or_list) is list:
            # First case of interest:
            # Belonging and Not Belonging to Set
            comparison_tokens = filter_spacing_tokens(token_or_list)
            if len(comparison_tokens) == 1:
                return cls._extract_reading_filters(comparison_tokens[0])
            elif len(comparison_tokens) in [3, 4]:
                identifiers = [
                    t for t in comparison_tokens if type(t) is Identifier
                ]
                filter_type = readingfilter_factory(comparison_tokens)
                if filter_type:
                    return [
                        (
                            identifiers[0].get_parent_name(),
                            filter_type(comparison_tokens),
                        )
                    ]

        return []

    @classmethod
    def _process_filters_for_reading(
        cls, tokens: list[Token]
    ) -> list[tuple[str, ReadingFilter]]:
        """
        Filters the tokens after the WHERE that might help deciding which
        partition files must be read, optimizing reading time.

        Filters that are considered:

        - Column equality to constant
        - Column unequality to constant
        - Column belonging to set
        - Column not belonging to set

        """

        where_tokens = [t for t in tokens if type(t) is Where]
        if len(where_tokens) > 0:
            splitted_tokens = split_token_list_in_and_or_keywords(
                filter_spacing_and_punctuation_tokens(
                    where_tokens[0].tokens[1:]
                )
            )
        else:
            splitted_tokens = []
        filtermaps: list[tuple[str, ReadingFilter]] = []
        for token_set in splitted_tokens:
            t_map = cls._extract_reading_filters(token_set)
            filtermaps += t_map

        return filtermaps

    @classmethod
    def _process_filters_for_querying(
        cls, tokens: list[Token], tables_to_select: dict[str, str]
    ):
        def process_filters_to_pandas_query(
            token_or_list: Token | list[Token],
        ) -> str:
            logical_operator_mappings: dict[str, str] = {
                "=": "==",
                "AND": "&",
                "OR": "|",
                "NOT": "not",
                "IN": "in",
            }
            if (
                (type(token_or_list) is Parenthesis)
                or (type(token_or_list) is IdentifierList)
                or (type(token_or_list) is Comparison)
            ):
                return process_filters_to_pandas_query(
                    filter_spacing_tokens(token_or_list.tokens)
                )
            elif type(token_or_list) is list:
                return " ".join(
                    process_filters_to_pandas_query(t) for t in token_or_list
                )
            elif type(token_or_list) is Identifier:
                return column_name_with_alias(token_or_list, tables_to_select)
            elif type(token_or_list) is Token:
                value = str(token_or_list.normalized)
                return logical_operator_mappings.get(value, value)
            else:
                raise ValueError(
                    f"Unknown token type {type(token_or_list)} in "
                    f"process_filters_to_pandas_query"
                )

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
    def _read_files_with_partitions(
        cls,
        conn: Connection,
        partition_columns: dict[str, str],
        filters: list[ReadingFilter],
    ) -> list[str]:
        files_to_read: list[str] = []
        for c, c_type in partition_columns.items():
            partition_files = conn.list_partition_files(c)
            casting_func = casting_functions(c_type)
            # Builds a value: [files] map
            partition_maps: dict[Any, list[str]] = {}
            for partition_file in partition_files:
                v = casting_func(partition_value_in_file(partition_file, c))
                if v in partition_maps:
                    partition_maps[v].append(partition_file)
                else:
                    partition_maps[v] = [partition_file]
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
                for reading_filter in column_filters:
                    filtered_values += reading_filter.apply(
                        partition_values, casting_func
                    )
                filtered_values = list(set(filtered_values))
            # Find files with values
            value_files = []
            for v in filtered_values:
                value_files += partition_maps[v]
            files_to_read += value_files
        files_to_read = list(set(files_to_read))
        return files_to_read

    @classmethod
    def _process_select_from_table(
        cls,
        table: str,
        filters: list[ReadingFilter],
        columns: dict[str, str],
        conn: Connection,
    ):
        table_conn = conn.access(table)
        if not table_conn.schema.is_table:
            raise ValueError(f"Schema {table} is not a table")
        table_format = str(table_conn.schema.format)
        table_io = io_factory(table_format)

        # List partitioned columns from schema
        partition_columns: dict[str, str] = table_conn.schema.partition_keys

        # The main result is the list of filenames that must be read
        # and concatenated.
        files_to_read: list[str] = []
        if len(partition_columns) == 0:
            files_to_read.append(table)
        else:
            files_to_read += cls._read_files_with_partitions(
                table_conn, partition_columns, filters
            )

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
                if k in columns.keys() or (len(columns) == 0):
                    dff[k] = casting_func(v)
            if len(columns) == 0:
                dfs.append(dff.copy())
            else:
                dfs.append(dff[columns.keys()].copy())
        df = pd.concat(dfs, ignore_index=True)
        # Rename due columns
        df.rename(
            columns=columns,
            inplace=True,
        )
        return df

    @classmethod
    def _process_select(
        cls,
        tables_to_select: dict[str, str],
        reading_filters: list[tuple[str, ReadingFilter]],
        columns_in_each_table: dict[str, dict[str, str]],
        conn: Connection,
    ) -> list[pd.DataFrame]:
        """
        Processes the SELECT statement for each table separately,
        using the reading_filters for optimizing the file reading steps.
        """
        dfs: list[pd.DataFrame] = []
        for alias, name in tables_to_select.items():
            dfs.append(
                cls._process_select_from_table(
                    name,
                    [f[1] for f in reading_filters if f[0] == alias],
                    columns_in_each_table.get(alias, {}),
                    conn,
                )
            )
        return dfs

    @classmethod
    def _process_join_tables(
        cls,
        dfs: list[pd.DataFrame],
        table_join_mappings: list[tuple[str, str]],
    ) -> pd.DataFrame:
        """
        Processes the JOIN keywords in the query, joining the tables in the order
        they appear in the statement.
        """
        num_joins = len(table_join_mappings)
        if num_joins > 0:
            for i in range(num_joins):
                df_left = dfs[i]
                df_right = dfs[i + 1]
                df_right.set_index(table_join_mappings[i][1], inplace=True)
                dfs[i + 1] = df_left.join(
                    df_right, on=table_join_mappings[i][0], how="inner"
                )
            return dfs[-1]
        else:
            return dfs[0]

    @classmethod
    def process(
        cls, tokens: list[Token], conn: Connection
    ) -> pd.DataFrame | dict:
        tables_to_select = cls._process_table_identifiers(tokens)
        columns_in_each_table = cls._process_column_identifiers(
            tokens, tables_to_select
        )
        table_join_mappings = cls._process_join_mappings(
            tokens, tables_to_select
        )
        reading_filters = cls._process_filters_for_reading(tokens)
        querying_filters = cls._process_filters_for_querying(
            tokens, tables_to_select
        )
        dfs = cls._process_select(
            tables_to_select, reading_filters, columns_in_each_table, conn
        )
        df = cls._process_join_tables(dfs, table_join_mappings)
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
