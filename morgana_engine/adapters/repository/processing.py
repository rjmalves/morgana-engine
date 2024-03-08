from abc import ABC
from morgana_engine.adapters.repository.connection import Connection
from morgana_engine.adapters.repository.dataio import factory as io_factory
from morgana_engine.models.readingfilter import (
    type_factory as readingfilter_factory,
)
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_float_dtype as is_float
from pandas.api.types import is_integer_dtype as is_integer
from pandas.api.types import is_bool_dtype as is_boolean
from pandas.api.types import is_string_dtype as is_string

from sqlparse.sql import (  # type: ignore
    Token,
    Parenthesis,
    Comparison,
    Where,
    Identifier,
    IdentifierList,
)
from typing import Any, Union, Optional
from os.path import join
from morgana_engine.models.readingfilter import ReadingFilter
from morgana_engine.utils.types import casting_functions
from morgana_engine.models.parsedsql import Column, Table, QueryingFilter
from morgana_engine.utils.sql import (
    partitions_in_file,
    partition_value_in_file,
)
from morgana_engine.utils.sql import (
    column_from_token,
    table_from_column_token,
    add_column_information_from_token,
    filter_spacing_tokens,
    filter_punctuation_tokens,
    filter_spacing_and_punctuation_tokens,
    split_token_list_in_and_or_keywords,
)


class Processing(ABC):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        """
        Processes the tokens associated with the statement in the
        database specified in the connection.
        """
        raise NotImplementedError


class CREATE(Processing):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        raise NotImplementedError


class ALTER(Processing):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        raise NotImplementedError


class DROP(Processing):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        raise NotImplementedError


class INSERT(Processing):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        raise NotImplementedError


class UPDATE(Processing):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        raise NotImplementedError


class DELETE(Processing):
    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
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
    def _process_table_identifiers(cls, tokens: list[Token]) -> list[Table]:
        """
        Processes the tokens associated with the tables identifiers in
        order to obtain a list of tables that are related to the query.

        Parameters:
        -----------
        tokens :  list[Token]
            A list of tokens that were parsed from the query with the SELECT
            statement.

        Returns:
        --------
        list[Table]
            A list of Table objects with their names and aliases, but with
            no column information up to this point.

        """
        # Iterate on tokens and find the table identifiers
        identifier_list = [t for t in tokens if type(t) is Identifier]
        alias_map: dict[str, str] = {
            t.get_alias(): t.get_real_name() for t in identifier_list
        }
        return [
            Table(name=v, alias=k, columns=[]) for k, v in alias_map.items()
        ]

    @classmethod
    def _process_column_wildcard(
        cls,
        tables_to_select: list[Table],
        conn: Connection,
    ):
        """
        Accesses the schema data for filling all column information
        since the select data is a wildcard.

        Parameters:
        -----------
        tables_to_select : list[Table]
            A list of tables that are related to the query.
        conn : Connection
            The connection to the database where the tables are located.

        """
        for table in tables_to_select:
            table_conn = conn.access(table.name)
            if not table_conn.schema.is_table:
                raise ValueError(f"Schema {table.name} is not a table")
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
                    )
                )

    @classmethod
    def _process_column_identifiers(
        cls,
        tokens: list[Token],
        tables_to_select: list[Table],
        conn: Connection,
    ):
        """
        Processes the tokens associated with the column identifiers in
        order to obtain a map of table_alias: {column_alias: column_name}
        and add their information to the table list that was previously
        parsed from the query.

        Parameters:
        -----------
        tokens :  list[Token]
            A list of tokens that were parsed from the query
        tables_to_select : list[Table]
            A list of tables that are related to the query.
        conn : Connection
            The connection to the database where the tables are located.

        """
        if "*" in [t.normalized for t in tokens]:
            cls._process_column_wildcard(tables_to_select, conn)
        else:
            # Else, iterate on tokens and find the column identifiers
            identifier_list = [t for t in tokens if type(t) is IdentifierList][
                0
            ]
            identifier_tokens: list[Identifier] = [
                t for t in identifier_list.tokens if type(t) is Identifier
            ]
            # Adds the column information to the table list
            for t in identifier_tokens:
                add_column_information_from_token(t, tables_to_select)
            # Adds data type information from the schemas
            for table in tables_to_select:
                table_conn = conn.access(table.name)
                if not table_conn.schema.is_table:
                    raise ValueError(f"Schema {table.name} is not a table")
                for column in table.columns:
                    column_schemas = list(
                        filter(
                            lambda column_pair: column_pair[0] == column.name,
                            table_conn.schema.columns.items(),
                        )
                    )
                    partition_schemas = list(
                        filter(
                            lambda partition_pair: partition_pair[0]
                            == column.name,
                            table_conn.schema.partitions.items(),
                        )
                    )
                    if len(column_schemas) == 1:
                        column.type_str = column_schemas[0][1]
                        column.partition = False
                    if len(partition_schemas) == 1:
                        column.type_str = partition_schemas[0][1]
                        column.partition = True

        return tables_to_select

    @classmethod
    def _process_join_mappings(
        cls, tokens: list[Token], tables_to_select: list[Table]
    ) -> list[tuple[Column, Column]]:
        """
        Processes the tokens associated with the join mappings in order
        to obtain a map of each join type with the tables that must be joined.

        Parameters:
        -----------
        tokens :  list[Token]
            A list of tokens that were parsed from the query
        tables_to_select : list[Table]
            A list of tables that are related to the query.

        Returns:
        --------
        list[tuple[Column, Column]]
            A list of tuples that define tables that must be joined,
            specifying the joining columns.

        """

        def __find_column_by_token(
            token: Identifier, columns: list[Column]
        ) -> Optional[Column]:
            for column in columns:
                table_name_or_alias = token.get_parent_name()
                table_match = (
                    column.table_alias == table_name_or_alias
                    or column.table_name == table_name_or_alias
                )
                column_name_or_alias = token.get_real_name()
                column_match = (
                    column.alias == column_name_or_alias
                    or column.name == column_name_or_alias
                )
                if table_match and column_match:
                    return column
            return None

        def __find_table_and_column_by_token(
            token: Identifier, tables: list[Table]
        ) -> Column:
            all_columns = [c for t in tables for c in t.columns]
            c = __find_column_by_token(token, all_columns)
            if c:
                return c

            raise ValueError(f"Column {token.get_alias()} not found")

        comparison_tokens = [t for t in tokens if type(t) is Comparison]
        mappings: list[tuple[Column, Column]] = []
        for c in comparison_tokens:
            identifiers = [
                t
                for t in filter_punctuation_tokens(c.tokens)
                if type(t) is Identifier
            ]
            mappings.append(
                (
                    __find_table_and_column_by_token(
                        identifiers[0], tables_to_select
                    ),
                    __find_table_and_column_by_token(
                        identifiers[1], tables_to_select
                    ),
                )
            )
        return mappings

    @classmethod
    def _extract_reading_filters(
        cls,
        token_or_list: Token | list[Token],
        tables_to_select: list[Table],
    ) -> list[ReadingFilter]:
        """
        Processes the tokens after the WHERE for extracting filters
        for optimizing partition reading.

        Parameters:
        -----------
        token_or_list : Token | list[Token]
            A token or list of tokens that were parsed from the query
        tables_to_select : list[Table]
            A list of tables that are related to the query.

        Returns:
        --------
        list[ReadingFilter]
            A list of ReadingFilters that should be applied to the reading
            process of each table. The default table receives the `None` alias.

        """
        if type(token_or_list) is Parenthesis:
            splitted_list = split_token_list_in_and_or_keywords(
                filter_spacing_tokens(token_or_list.tokens)
            )
            filters = []
            for token_list in splitted_list:
                filters += cls._extract_reading_filters(
                    token_list, tables_to_select
                )
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
                # Find the table with the given alias
                table = table_from_column_token(
                    identifiers[0], tables_to_select
                )
                return [filter_type(comparison_tokens, table)]
        elif type(token_or_list) is list:
            # Second case of interest:
            # Belonging and Not Belonging to Set
            comparison_tokens = filter_spacing_tokens(token_or_list)
            if len(comparison_tokens) == 1:
                return cls._extract_reading_filters(
                    comparison_tokens[0], tables_to_select
                )
            elif len(comparison_tokens) in [3, 4]:
                identifiers = [
                    t for t in comparison_tokens if type(t) is Identifier
                ]
                filter_type = readingfilter_factory(comparison_tokens)
                if filter_type:
                    # Find the table with the given alias
                    table = table_from_column_token(
                        identifiers[0], tables_to_select
                    )
                    return [filter_type(comparison_tokens, table)]

        return []

    @classmethod
    def _process_filters_for_reading(
        cls,
        tokens: list[Token],
        tables_to_select: list[Table],
    ) -> list[ReadingFilter]:
        """
        Filters the tokens after the WHERE that might help deciding which
        partition files must be read, optimizing reading time.

        Filters that are considered:

        - Column equality to constant
        - Column unequality to constant
        - Column belonging to set
        - Column not belonging to set

        Parameters:
        -----------
        tokens :  list[Token]
            A list of tokens that were parsed from the query
        tables_to_select : list[Table]
            A list of tables that are related to the query.

        Returns:
        --------
        list[ReadingFilter]
            A list of ReadingFilters that should be applied to the reading
            process of each table. The default table receives the `None` alias.

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
        filtermaps: list[ReadingFilter] = []
        for token_set in splitted_tokens:
            t_map = cls._extract_reading_filters(token_set, tables_to_select)
            filtermaps += t_map

        return filtermaps

    @classmethod
    def _process_filters_for_querying(
        cls, tokens: list[Token], tables_to_select: list[Table]
    ) -> list[Union[QueryingFilter, Column, str]]:
        """
        Filters the tokens after the WHERE that should be used for
        querying the DataFrame.

        Parameters:
        -----------
        tokens :  list[Token]
            A list of tokens that were parsed from the query
        tables_to_select : list[Table]
            A list of tables that are related to the query.

        Returns:
        --------
        list[Union[QueryingFilter, Column, str]]
            A collection of elements that will be used to
            compose the query string (column_name, operator, value) or
            just a separator "&", "|" or "~".

        """

        def process_filters_to_pandas_query(
            token_or_list: Token | list[Token],
        ) -> Union[QueryingFilter, Column, str]:
            """
            Filters the tokens after the WHERE that should be used for
            querying the DataFrame.

            Parameters:
            -----------
            token_or_list :  Token | list[Token]
                A token or list of tokens that were parsed from the query

            Returns:
            --------
            QueryingFilter | Column | str
                An element that compose a filter for the query string.

            """
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
                elements = [
                    process_filters_to_pandas_query(t) for t in token_or_list
                ]
                return QueryingFilter(*elements)  # type: ignore

            elif type(token_or_list) is Identifier:
                return column_from_token(token_or_list, tables_to_select)
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
            return pandas_query_elements
        else:
            return []

    @classmethod
    def _dataframe_column_type_casting_keyword(cls, column: pd.Series) -> str:
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

    @classmethod
    def _compose_query_and_query_dataframe(
        cls,
        df: pd.DataFrame,
        parsed_filters: list[Union[QueryingFilter, Column, str]],
    ) -> pd.DataFrame:

        def __cast_unquoting_value(value: str, column: str) -> Any:
            unquoted_value = value.replace("'", "").replace('"', "")
            casting_function_keyword = (
                cls._dataframe_column_type_casting_keyword(df[column])
            )
            return casting_functions(casting_function_keyword)(unquoted_value)

        casted_filter_values: list[Any] = []
        query_string_parts: list[str] = []
        num_filters = 0
        for f in parsed_filters:
            if isinstance(f, QueryingFilter):
                query_string_part = (
                    f"{f.column.fullname} {f.operator}"
                    + f" @casted_filter_values[{num_filters}]"
                )
                casted_filter_values.append(
                    __cast_unquoting_value(f.value, f.column.fullname)
                )
                query_string_parts.append(query_string_part)
                num_filters += 1
            else:
                query_string_parts.append(str(f))
        query_string = " ".join(query_string_parts)
        if num_filters > 0:
            # All filters have a (column, operator, value) format
            df = df.query(query_string)
            return df
        else:
            return df

    @classmethod
    def _read_files_with_partitions(
        cls,
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

    @classmethod
    def _process_select_from_table(
        cls, table: Table, filters: list[ReadingFilter], conn: Connection
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

    @classmethod
    def _process_select_from_tables(
        cls,
        tables_to_select: list[Table],
        reading_filters: list[ReadingFilter],
        conn: Connection,
    ) -> dict:
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
        for table in tables_to_select:
            name = table.name
            table_select_result = cls._process_select_from_table(
                table,
                [f for f in reading_filters if f.column.table_name == name],
                conn,
            )
            files += table_select_result["processedFiles"]
            dfs.append(table_select_result["data"])
        return {
            "processedFiles": files,
            "data": dfs,
        }

    @classmethod
    def _process_join_tables(
        cls,
        dfs: list[pd.DataFrame],
        table_join_mappings: list[tuple[Column, Column]],
    ) -> pd.DataFrame:
        """
        Processes the JOIN keywords in the query, joining the tables in the order
        they appear in the statement.

        Parameters:
        -----------
        dfs : list[pd.DataFrame]
            List of dataframes that were read from each table
        table_join_mappings : list[tuple[Column, Column]]
            A mapping of table columns that should be joined.

        Returns:
        --------
        pd.DataFrame
            The dataframe resulting from the JOIN operations.

        """
        num_joins = len(table_join_mappings)
        if num_joins > 0:
            for i in range(num_joins):
                df_left = dfs[i]
                left_col = table_join_mappings[i][0]
                df_right = dfs[i + 1]
                right_col = table_join_mappings[i][1]
                df_right.set_index(right_col.fullname, inplace=True)
                dfs[i + 1] = df_left.join(
                    df_right,
                    on=left_col.fullname,
                    how="inner",
                )
            return dfs[-1]
        else:
            return dfs[0]

    @classmethod
    def process(cls, tokens: list[Token], conn: Connection) -> dict:
        """
        Processes a list of tokens that were parsed from a SELECT statement.
        The processing includes the following steps:
            1. Processing the table identifiers
            2. Processing the column identifiers
            3. Processing the join mappings
            4. Processing the filters for reading (optimizing partitions)
            5. Processing the filters for querying (pandas query string)
            6. Processing the SELECT of each table (list of DataFrames)
            7. Processing the JOIN keywords (single DataFrame)
            8. Querying the DataFrame if needed

        Parameters:
        -----------
        tokens : list[Token]
            A list of tokens that were parsed from the query with the SELECT
            statement.
        conn: Connection
            The connection to the database where the tables are located.

        Returns:
        --------
        dict
            A dict with a the result of the query processing. Might contain
            a dataframe with the requested data or an error code and message.

        """
        tables_to_select = cls._process_table_identifiers(tokens)
        tables_to_select = cls._process_column_identifiers(
            tokens, tables_to_select, conn
        )
        table_join_mappings = cls._process_join_mappings(
            tokens, tables_to_select
        )
        reading_filters = cls._process_filters_for_reading(
            tokens, tables_to_select
        )
        querying_filters = cls._process_filters_for_querying(
            tokens, tables_to_select
        )
        select_result = cls._process_select_from_tables(
            tables_to_select, reading_filters, conn
        )
        df = cls._process_join_tables(
            select_result["data"], table_join_mappings
        )
        # The query string must be built here, referencing external
        # variables with the correct types.
        df = cls._compose_query_and_query_dataframe(df, querying_filters)
        return {
            "statusCode": 200,
            "data": df,
            "processedFiles": select_result["processedFiles"],
        }


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
