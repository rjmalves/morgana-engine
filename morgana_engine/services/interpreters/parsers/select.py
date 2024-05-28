from morgana_engine.models.sql import (
    SQLTokenType,
    SQLToken,
    SQLStatement,
    SQLParser,
    ParsingResult,
    OPERATION_TOKEN_TYPES,
)

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_float_dtype as is_float
from pandas.api.types import is_integer_dtype as is_integer
from pandas.api.types import is_bool_dtype as is_boolean
from pandas.api.types import is_string_dtype as is_string
from morgana_engine.adapters.repository.connection import Connection
from morgana_engine.adapters.repository.dataio import factory as io_factory
from morgana_engine.models.readingfilter import type_factory, ReadingFilter
from morgana_engine.models.parsedsql import Column, Table, QueryingFilter
from morgana_engine.utils.types import casting_functions
from morgana_engine.utils.sql import (
    partitions_in_file,
    partition_value_in_file,
)
from os.path import join
from typing import Optional, Union, List, Tuple, Any


class SELECTParser(SQLParser):

    @staticmethod
    def match_statement(statement: SQLStatement) -> bool:
        return statement.tokens[0].type == SQLTokenType.SELECT

    def __validate_select_from(self) -> Optional[ParsingResult]:
        tokens = self.statement.tokens
        select_tokens = list(
            filter(lambda t: t.type == SQLTokenType.SELECT, tokens)
        )
        if len(select_tokens) != 1:
            return ParsingResult(
                status=False,
                message="The statement must contain 1 SELECT",
            )
        from_tokens = list(
            filter(lambda t: t.type == SQLTokenType.FROM, tokens)
        )
        if len(from_tokens) != 1:
            return ParsingResult(
                status=False,
                message="The statement must contain FROM",
                data=None,
            )
        self.__select_index = tokens.index(select_tokens[0])
        self.__from_index = tokens.index(from_tokens[0])
        last_index = len(tokens)
        if self.__from_index - self.__select_index < 2:
            return ParsingResult(
                status=False,
                message="The statement must contain at least"
                + " one entity between SELECT and FROM",
                data=None,
            )
        if last_index - self.__from_index < 2:
            return ParsingResult(
                status=False,
                message="The statement must contain at least"
                + " one entity after FROM",
                data=None,
            )
        return None

    def __validate_where(self) -> Optional[ParsingResult]:
        tokens = self.statement.tokens
        where_tokens = list(
            filter(lambda t: t.type == SQLTokenType.WHERE, tokens)
        )
        if len(where_tokens) > 1:
            return ParsingResult(
                status=False,
                message="The statement may contain 1 WHERE",
                data=None,
            )
        self.__filtered = len(where_tokens) == 1
        self.__where_index = (
            tokens.index(where_tokens[0]) if self.__filtered else None
        )
        if self.__filtered:
            last_index = len(tokens)
            if last_index - self.__where_index < 2:
                return ParsingResult(
                    status=False,
                    message="The statement must contain at least"
                    + " one entity after WHERE",
                    data=None,
                )
        return None

    @staticmethod
    def __split_by_token_type(
        tokens: List[SQLToken],
        token_types: Union[SQLTokenType, List[SQLTokenType]],
    ) -> List[List[SQLToken]]:
        if isinstance(token_types, SQLTokenType):
            token_types = [token_types]
        tokens_of_type = list(filter(lambda t: t.type in token_types, tokens))
        tokens_indices = (
            [-1] + [tokens.index(c) for c in tokens_of_type] + [len(tokens)]
        )
        splitting_indices = []
        for i in range(len(tokens_indices) - 1):
            splitting_indices.append(
                (tokens_indices[i] + 1, tokens_indices[i + 1])
            )
        return [tokens[s:e] for s, e in splitting_indices]

    def __get_querying_tables(self) -> Optional[ParsingResult]:

        self.tables: List[Table] = []
        # Gets tokens between FROM and WHERE (or the end)
        # for considering as columns
        last_index = (
            self.__where_index
            if self.__filtered
            else len(self.statement.tokens)
        )
        tokens = self.statement.tokens[self.__from_index + 1 : last_index]

        joining_tokens = self.__split_by_token_type(tokens, SQLTokenType.JOIN)

        for table_token_group in joining_tokens:
            table_tokens = self.__split_by_token_type(
                table_token_group, SQLTokenType.COMMA
            )
            for column_token_group in table_tokens:
                aliases_tokens = self.__split_by_token_type(
                    column_token_group, SQLTokenType.AS
                )
                table_name = aliases_tokens[0][0].text
                table_alias = (
                    aliases_tokens[-1][0].text
                    if len(aliases_tokens) > 1
                    else None
                )

                self.tables.append(
                    Table(name=table_name, alias=table_alias, columns=[])
                )

        # TODO - maybe needs to parse for "ON" and "JOIN" tokens
        # for supporting the case where the tables don't have
        # aliases
        return None

    def __get_schema_elements(self) -> Optional[ParsingResult]:
        for table in self.tables:
            table_conn = self.conn.access(table.name)
            if not table_conn.schema.is_table:
                return ParsingResult(
                    status=False,
                    message=f"Table {table.name} not found",
                    data=None,
                )
            for (
                column_name,
                column_type,
            ) in table_conn.schema.columns.items():
                table.columns.append(
                    Column(
                        name=column_name,
                        alias=None,
                        type_str=column_type,
                        table_name=table.name,
                        table_alias=table.alias,
                        has_parent_in_token=False,
                        partition=False,
                        querying=False,
                    )
                )
            for (
                partition_name,
                partition_type,
            ) in table_conn.schema.partitions.items():
                table.columns.append(
                    Column(
                        name=partition_name,
                        alias=None,
                        type_str=partition_type,
                        table_name=table.name,
                        table_alias=table.alias,
                        has_parent_in_token=False,
                        partition=True,
                        querying=False,
                    )
                )

    def __get_table_from_token_list(
        self, name_or_alias: str
    ) -> Union[Table, ParsingResult]:
        # Finds the table with the given name / alias
        tables = list(
            filter(
                lambda t: t.name == name_or_alias or t.alias == name_or_alias,
                self.tables,
            )
        )
        if len(tables) != 1:
            return ParsingResult(
                status=False,
                message=f"Table {name_or_alias} not found or is ambiguous",
                data=None,
            )
        return tables[0]

    def __get_column_from_token_list(
        self, tokens: List[SQLToken]
    ) -> Union[Column, ParsingResult]:
        columns_with_table = self.__split_by_token_type(
            tokens, SQLTokenType.DOT
        )
        if len(columns_with_table) > 1:
            table_name_or_alias = columns_with_table[0][0].text
            column_name = columns_with_table[1][0].text
            has_parent_in_token = True

            # Finds the table with the given name / alias
            table = self.__get_table_from_token_list(table_name_or_alias)
            if isinstance(table, ParsingResult):
                return table
        else:
            table = self.tables[0]
            column_name = columns_with_table[0][0].text
            has_parent_in_token = False

        table_column = list(
            filter(lambda c: c.name == column_name, table.columns)
        )
        if len(table_column) != 1:
            return ParsingResult(
                status=False,
                message=f"Column {column_name} not found in table {table.name}",
                data=None,
            )
        table_column[0].has_parent_in_token = has_parent_in_token
        return table_column[0]

    def __get_querying_columns(self) -> Optional[ParsingResult]:
        # Gets tokens between SELECT and FROM
        # for considering as columns
        tokens = self.statement.tokens[
            self.__select_index + 1 : self.__from_index
        ]
        # Splits the tokens by commas
        # into sublists for each column,
        # possibly with aliases
        column_tokens = self.__split_by_token_type(tokens, SQLTokenType.COMMA)
        for column_token_group in column_tokens:
            aliases_tokens = self.__split_by_token_type(
                column_token_group, SQLTokenType.AS
            )
            column_alias = (
                aliases_tokens[-1][0].text if len(aliases_tokens) > 1 else None
            )
            r = self.__get_column_from_token_list(aliases_tokens[0])
            if isinstance(r, ParsingResult):
                return r
            else:
                table_column = r
                table_column.querying = True
                table_column.alias = column_alias

        return None

    def __get_joining_columns(self) -> Optional[ParsingResult]:

        self.joining_columns: List[Tuple[Column, Column, str]] = []
        last_index = (
            self.__where_index
            if self.__filtered
            else len(self.statement.tokens)
        )
        tokens = self.statement.tokens[self.__from_index + 1 : last_index]

        joining_tokens = self.__split_by_token_type(tokens, SQLTokenType.JOIN)
        num_joins = len(joining_tokens) - 1

        if num_joins == 0:
            return None

        for left_joining_tokens, right_joining_tokens in zip(
            joining_tokens[:-1], joining_tokens[1:]
        ):
            tables: List[Table] = []
            for token_set in [left_joining_tokens, right_joining_tokens]:
                aliases_tokens = self.__split_by_token_type(
                    token_set, SQLTokenType.AS
                )
                table_name = aliases_tokens[0][0].text
                table = self.__get_table_from_token_list(table_name)
                if isinstance(table, ParsingResult):
                    return table

                tables.append(table)

            join_kind_token = left_joining_tokens[-1]

            on_tokens = self.__split_by_token_type(
                right_joining_tokens, SQLTokenType.ON
            )[1]

            joining_column_tokens = self.__split_by_token_type(
                on_tokens, SQLTokenType.EQUALS
            )
            columns = [
                self.__get_column_from_token_list(column_tokens)
                for column_tokens in joining_column_tokens
            ] + [join_kind_token.text.lower()]
            self.joining_columns.append(tuple(columns))

        return None

    def __filter_querying_columns(self) -> Optional[ParsingResult]:
        for table in self.tables:
            table.columns = list(filter(lambda c: c.querying, table.columns))

    def __get_reading_filters(self) -> Optional[ParsingResult]:

        def __recursive_get_reading_filters(
            tokens: List[SQLToken], context_depths: np.ndarray
        ) -> Union[Tuple[List[SQLToken], np.ndarray], ParsingResult]:
            # Get deepest context and extract tokens
            max_context_depth = max(context_depths)
            indices = np.where(context_depths == max_context_depth)[0]
            internal_tokens: List[SQLToken] = [tokens[i] for i in indices]
            initial_index = indices[0]
            final_index = indices[-1]
            before_tokens, before_depths = (
                tokens[:initial_index],
                context_depths[:initial_index],
            )
            after_tokens, after_depths = (
                tokens[final_index + 1 :],
                context_depths[final_index + 1 :],
            )
            if SQLTokenType.LPAREN in [t.type for t in internal_tokens]:
                initial_index += 1
                internal_tokens = internal_tokens[1:]
            if SQLTokenType.RPAREN in [t.type for t in internal_tokens]:
                final_index -= 1
                internal_tokens = internal_tokens[:-1]

            if all(
                [
                    t.type in [SQLTokenType.ENTITY, SQLTokenType.COMMA]
                    for t in internal_tokens
                ]
                + [len(tokens) > 0]
            ):
                # If there are only entities and commas, it is a list
                # of values.
                value_list_token = SQLToken(
                    SQLTokenType.ENTITY,
                    text=" ".join([t.text for t in internal_tokens]),
                )
                tokens_to_add = [value_list_token]
                depths_to_add = np.array(
                    [max_context_depth - 1] * len(tokens_to_add), dtype=int
                )
            else:
                tokens_to_add = []
                depths_to_add = []
                # Split be AND and OR operators
                subfilters = self.__split_by_token_type(
                    internal_tokens, [SQLTokenType.AND, SQLTokenType.OR]
                )
                # Create each reading filter
                for f in subfilters:
                    operation = list(
                        filter(
                            lambda t: t.type in OPERATION_TOKEN_TYPES,
                            f,
                        )
                    )
                    if len(operation) == 0:
                        return ParsingResult(
                            status=False,
                            message="No operation found in filter"
                            + f" {[str(t) for t in f]}",
                            data=None,
                        )
                    else:
                        column_tokens = f[: f.index(operation[0])]
                        value_tokens = f[f.index(operation[-1]) + 1 :]
                        if len(operation) == 2:
                            if not all(
                                [
                                    t.type
                                    in [
                                        SQLTokenType.NOT,
                                        SQLTokenType.IN,
                                    ]
                                    for t in operation
                                ]
                            ):
                                return ParsingResult(
                                    status=False,
                                    message="Invalid operation found in filter"
                                    + f" {[str(t) for t in operation]}",
                                    data=None,
                                )
                            else:
                                operation = [
                                    SQLToken(
                                        SQLTokenType.NOT_IN, text="NOT IN"
                                    )
                                ]
                    column_or_result = self.__get_column_from_token_list(
                        column_tokens
                    )
                    if isinstance(column_or_result, ParsingResult):
                        return column_or_result
                    else:
                        column = column_or_result
                    reading_filter_type = type_factory(operation[0])
                    r = reading_filter_type(column, operation[0], value_tokens)
                    self.reading_filters.append(r)

            return (
                before_tokens + tokens_to_add + after_tokens,
                np.concatenate([before_depths, depths_to_add, after_depths]),
            )

        def __add_token_context_depths(tokens: List[SQLToken]) -> np.ndarray:
            context_depths: np.ndarray = np.zeros_like(tokens, dtype=int)
            current_context = 0
            for i, t in enumerate(tokens):
                if t.type == SQLTokenType.LPAREN:
                    current_context += 1
                context_depths[i] = current_context
                if t.type == SQLTokenType.RPAREN:
                    current_context -= 1
            return context_depths

        tokens = self.statement.tokens[self.__where_index + 1 :]
        context_depths = __add_token_context_depths(tokens)

        # Recursively parses tokens and context depths, replacing
        # inner contexts with single tokens whenever possible
        # and creating the reading filters

        self.reading_filters: List[ReadingFilter] = []
        while len(tokens) > 0:
            r = __recursive_get_reading_filters(tokens, context_depths)
            if isinstance(r, ParsingResult):
                return r
            else:
                tokens, context_depths = r

    def __get_querying_filters(self) -> Optional[ParsingResult]:

        def __recursive_get_querying_filters(
            tokens: List[SQLToken], context_depths: np.ndarray
        ) -> Union[Tuple[List[SQLToken], np.ndarray], ParsingResult]:
            # Get deepest context and extract tokens
            max_context_depth = max(context_depths)
            indices = np.where(context_depths == max_context_depth)[0]
            internal_tokens: List[SQLToken] = [tokens[i] for i in indices]
            initial_index = indices[0]
            final_index = indices[-1]
            before_tokens, before_depths = (
                tokens[:initial_index],
                context_depths[:initial_index],
            )
            after_tokens, after_depths = (
                tokens[final_index + 1 :],
                context_depths[final_index + 1 :],
            )
            if SQLTokenType.LPAREN in [t.type for t in internal_tokens]:
                initial_index += 1
                internal_tokens = internal_tokens[1:]
            if SQLTokenType.RPAREN in [t.type for t in internal_tokens]:
                final_index -= 1
                internal_tokens = internal_tokens[:-1]

            if all(
                [
                    t.type in [SQLTokenType.ENTITY, SQLTokenType.COMMA]
                    for t in internal_tokens
                ]
                + [len(tokens) > 0]
            ):
                # If there are only entities and commas, it is a list
                # of values.
                value_list_token = SQLToken(
                    SQLTokenType.ENTITY,
                    text=" ".join([t.text for t in internal_tokens]),
                )
                tokens_to_add = [value_list_token]
                depths_to_add = np.array(
                    [max_context_depth - 1] * len(tokens_to_add), dtype=int
                )
            else:
                tokens_to_add = []
                depths_to_add = []
                # Split be AND and OR operators
                subfilters = self.__split_by_token_type(
                    internal_tokens, [SQLTokenType.AND, SQLTokenType.OR]
                )
                filter_delimiters = [
                    t
                    for t in internal_tokens
                    if t.type in [SQLTokenType.AND, SQLTokenType.OR]
                ]
                # Create each reading filter
                for i, f in enumerate(subfilters):
                    operation = list(
                        filter(
                            lambda t: t.type in OPERATION_TOKEN_TYPES,
                            f,
                        )
                    )
                    if len(operation) == 0:
                        return ParsingResult(
                            status=False,
                            message="No operation found in filter"
                            + f" {[str(t) for t in f]}",
                            data=None,
                        )
                    else:
                        column_tokens = f[: f.index(operation[0])]
                        value_tokens = f[f.index(operation[-1]) + 1 :]
                        if len(operation) == 2:
                            if not all(
                                [
                                    t.type
                                    in [
                                        SQLTokenType.NOT,
                                        SQLTokenType.IN,
                                    ]
                                    for t in operation
                                ]
                            ):
                                return ParsingResult(
                                    status=False,
                                    message="Invalid operation found in filter"
                                    + f" {[str(t) for t in operation]}",
                                    data=None,
                                )
                            else:
                                operation = [
                                    SQLToken(
                                        SQLTokenType.NOT_IN, text="NOT IN"
                                    )
                                ]
                    column_or_result = self.__get_column_from_token_list(
                        column_tokens
                    )
                    if isinstance(column_or_result, ParsingResult):
                        return column_or_result
                    else:
                        column = column_or_result
                    logical_operator_mappings: dict[str, str] = {
                        "=": "==",
                        "AND": "&",
                        "OR": "|",
                        "NOT IN": "not in",
                        "IN": "in",
                    }
                    operation_token = operation[0]
                    operator = logical_operator_mappings.get(
                        operation_token.text,
                        operation_token.text,
                    )
                    value_str = (
                        "(" + value_tokens[0].text + ")"
                        if operation_token.type
                        in [SQLTokenType.IN, SQLTokenType.NOT_IN]
                        else value_tokens[0].text
                    )
                    q = QueryingFilter(column, operator, value_str)
                    self.querying_filters.append(q)
                    if i < len(filter_delimiters):
                        delimiter = logical_operator_mappings.get(
                            filter_delimiters[i].text,
                            filter_delimiters[i].text,
                        )
                        self.querying_filters.append(delimiter)

            return (
                before_tokens + tokens_to_add + after_tokens,
                np.concatenate([before_depths, depths_to_add, after_depths]),
            )

        def __add_token_context_depths(tokens: List[SQLToken]) -> np.ndarray:
            context_depths: np.ndarray = np.zeros_like(tokens, dtype=int)
            current_context = 0
            for i, t in enumerate(tokens):
                if t.type == SQLTokenType.LPAREN:
                    current_context += 1
                context_depths[i] = current_context
                if t.type == SQLTokenType.RPAREN:
                    current_context -= 1
            return context_depths

        tokens = self.statement.tokens[self.__where_index + 1 :]
        context_depths = __add_token_context_depths(tokens)

        # Recursively parses tokens and context depths, replacing
        # inner contexts with single tokens whenever possible
        # and creating the reading filters

        self.querying_filters: List[Union[QueryingFilter, str]] = []
        while len(tokens) > 0:
            r = __recursive_get_querying_filters(tokens, context_depths)
            if isinstance(r, ParsingResult):
                return r
            else:
                tokens, context_depths = r

    def validate(self) -> Optional[ParsingResult]:

        validators = [
            self.__validate_select_from,
            self.__validate_where,
            self.__get_querying_tables,
            self.__get_schema_elements,
            self.__get_querying_columns,
            self.__filter_querying_columns,
            self.__get_joining_columns,
            self.__get_reading_filters,
            self.__get_querying_filters,
        ]
        for v in validators:
            r = v()
            if r:
                return r

        return None

    def __dataframe_column_type_casting_keyword(
        self, column: pd.Series
    ) -> str:
        """
        Checks the column type among all supported data types in the DataFrame
        and returns the keyword that should by used by the casting function
        to perform the correct casting, enabling the query string.
        """
        column_type_inference_function_map = {
            "int": is_integer,
            "float": is_float,
            "datetime": is_datetime,
            "bool": is_boolean,
            "string": is_string,
        }
        for (
            typestring,
            checking_function,
        ) in column_type_inference_function_map.items():
            if checking_function(column):
                return typestring
        return "string"

    def __compose_query_and_query_dataframe(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:

        def __cast_unquoting_value(f: QueryingFilter) -> Any:
            value = f.value
            column = f.column.fullname
            casting_function_keyword = (
                self.__dataframe_column_type_casting_keyword(df[column])
            )
            unquoted_value = value.replace("'", "").replace('"', "").strip()
            if f.is_collection:
                str_values = (
                    unquoted_value.replace("(", "").replace(")", "").split(",")
                )
                return tuple(
                    [
                        casting_functions(casting_function_keyword)(v.strip())
                        for v in str_values
                        if len(v) > 0
                    ]
                )

            else:
                return casting_functions(casting_function_keyword)(
                    unquoted_value
                )

        casted_filter_values: list[Any] = []
        query_string_parts: list[str] = []
        num_filters = 0
        for f in self.querying_filters:
            if isinstance(f, QueryingFilter):
                query_string_part = (
                    "("
                    + f"{f.column.fullname} {f.operator}"
                    + f" @casted_filter_values[{num_filters}]"
                    + ")"
                )
                casted_filter_values.append(__cast_unquoting_value(f))
                query_string_parts.append(query_string_part)
                num_filters += 1
            else:
                query_string_parts.append(f)

        query_string = " ".join(query_string_parts)
        if num_filters > 0:
            # All filters have a (column, operator, value) format
            df = df.query(query_string)
            return df
        else:
            return df

    def __read_files_with_partitions(
        self,
        conn: Connection,
        partition_columns: dict[str, str],
        filters: list[ReadingFilter],
    ) -> list[str]:
        """
        Lists the files that must be read from a table with partitions,
        considering the desired filters for each partitioned column.

        Parameters:
        -----------
        conn : Connection
            The connection to the database where the table is located.
        partition_columns :  dict[str, str]
            A mapping between columns and their data types, for each
            column that define a partition.
        filters : list[ReadingFilter]
            A list of ReadingFilter objects that are associated with the
            table, for optimize partition reading.

        Returns:
        --------
        list[str]
            The list of filenames that must be read.

        """

        table_name = conn.schema.name
        table_format = conn.schema.file_type
        files_to_read: list[str] = []
        for c, c_type in partition_columns.items():
            partition_files = conn.list_partition_files(c)
            casting_func = casting_functions(c_type)
            # Builds a value: [files] map
            partition_maps: dict[Any, list[str]] = {}
            for partition_file in partition_files:
                # Remove table name and extension from filename
                parsed_filename = partition_file.lstrip(table_name).strip(
                    table_format
                )
                v = casting_func(partition_value_in_file(parsed_filename, c))
                if v in partition_maps:
                    partition_maps[v].append(partition_file)
                else:
                    partition_maps[v] = [partition_file]
            # List all possible values
            partition_values = [
                casting_func(v) for v in list(partition_maps.keys())
            ]
            # Apply filters
            column_filters = [f for f in filters if f.column.name == c]
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

    def __process_select_from_table(
        self, table: Table, filters: list[ReadingFilter], conn: Connection
    ) -> dict:
        """
        Processes the content of the SELECT statement with respect
        to a single table, reading the files that are necessary, casting
        data types if needed and returning the requested columns.

        Parameters:
        -----------
        table :  Table
            The table object to be read.
        filters : list[ReadingFilter]
            A list of ReadingFilter objects that are associated with the
            table, for optimize partition reading.

        Returns:
        --------
        dict
            A dict with the dataframe with the requested data from the table
            and some metadata regarding the reading process.

        """
        table_conn = conn.access(table.name)
        if not table_conn.schema.is_table:
            raise ValueError(f"Schema {table} is not a table")
        table_format = str(table_conn.schema.file_type)
        table_io = io_factory(table_format)

        # List partitioned columns from schema

        columns: list[Column] = table.columns
        column_mappings: dict[str, str] = {c.name: c.fullname for c in columns}

        partition_columns: dict[str, str] = table_conn.schema.partitions
        # The main result is the list of filenames that must be read
        # and concatenated.
        files_to_read: list[str] = []
        if len(partition_columns) == 0:
            files_to_read.append(table.name)
        else:
            files_to_read += self.__read_files_with_partitions(
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
                    table_conn.schema.partitions[k]
                )
                if k in column_mappings.keys():
                    dff[k] = casting_func(v)
            dfs.append(dff[list(column_mappings.keys())].copy())
        df = pd.concat(dfs, ignore_index=True)

        # List non-partitioned columns from schema
        non_partitioned_columns: dict[str, str] = table_conn.schema.columns
        # Filters for the columns that have been queried
        non_partitioned_columns = {
            k: v for k, v in non_partitioned_columns.items() if k in df.columns
        }
        for col, col_type in non_partitioned_columns.items():
            # Casts columns to the right types when date or datetime
            if pd.api.types.is_object_dtype(df[col]) and col_type in [
                "date",
                "datetime",
            ]:
                df[col] = pd.to_datetime(df[col])

        # Rename due columns
        df = df.rename(columns=column_mappings)

        return {
            "processedFiles": files_to_read,
            "data": df,
        }

    def __process_select_from_tables(self) -> dict:
        """
        Processes the SELECT statement for each table separately,
        using the reading_filters for optimizing the file reading steps.

        Parameters:
        -----------
        tables_to_select :  dict[str, str]
            Mappings of table aliases to table names for each table
            that must be read, as extracted from the query.
        reading_filters : list[tuple[str, ReadingFilter]]
            A mapping between table aliases and a list of ReadingFilter objects
            that are associated with each table.
        conn: Connection
            The connection to the database where the tables are located.


        Returns:
        --------
        dict
            A dict with the dataframe with the requested data from the table
            and some metadata regarding the reading process.

        """
        files: list[str] = []
        dfs: list[pd.DataFrame] = []
        for table in self.tables:
            name = table.name
            table_select_result = self.__process_select_from_table(
                table,
                [
                    f
                    for f in self.reading_filters
                    if f.column.table_name == name
                ],
                self.conn,
            )
            files += table_select_result["processedFiles"]
            dfs.append(table_select_result["data"])
        return {
            "processedFiles": files,
            "data": dfs,
        }

    def __join_tables(
        self,
        dfs: list[pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Processes the JOIN keywords in the query, joining the tables in the order
        they appear in the statement.

        Parameters:
        -----------
        dfs : list[pd.DataFrame]
            List of dataframes that were read from each table

        Returns:
        --------
        pd.DataFrame
            The dataframe resulting from the JOIN operations.

        """

        num_joins = len(self.joining_columns)
        if num_joins > 0:
            for i in range(num_joins):
                df_left = dfs[i]
                left_col = self.joining_columns[i][0]
                df_right = dfs[i + 1]
                right_col = self.joining_columns[i][1]
                join_kind = self.joining_columns[i][2]
                df_right.set_index(right_col.fullname, inplace=True)
                dfs[i + 1] = df_left.join(
                    df_right,
                    on=left_col.fullname,
                    how=join_kind,
                )
            return dfs[-1]
        else:
            return dfs[0]

    def parse(self) -> ParsingResult:

        select_result = self.__process_select_from_tables()
        df = self.__join_tables(select_result["data"])
        df = self.__compose_query_and_query_dataframe(df)
        return {
            "statusCode": 200,
            "data": df,
            "processedFiles": select_result["processedFiles"],
        }

        pass
