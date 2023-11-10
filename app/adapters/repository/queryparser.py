from abc import ABC
from app.adapters.repository.connection import Connection
from app.adapters.repository.dataio import factory as io_factory
import pandas as pd
from sqlparse.sql import Token
from sqlparse.sql import (
    Comparison,
    Where,
    Identifier,
    IdentifierList,
)
from typing import Any
from os.path import join
from app.utils.sql import (
    identifierlist2dict,
    aliases2dict,
    join_comparison_mapping,
    where2filtermap,
    where2pandas,
)
from app.models.readingfilter import ReadingFilter
from app.utils.types import casting_functions
from app.utils.sql import partitions_in_file, partition_value_in_file


class QueryParser(ABC):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class CREATEParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class ALTERParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class DROPParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class INSERTParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class UPDATEParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class DELETEParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        raise NotImplementedError


class SELECTParser(QueryParser):
    @classmethod
    def __parse_select_from_table(
        cls,
        table: str,
        filters: list[ReadingFilter],
        columns: dict[str, str],
        conn: Connection,
    ):
        table_conn = conn.access(table)
        table_io = io_factory(table_conn.schema["format"])

        # List partitioned columns from schema
        partition_columns: dict[str, str] = {}
        for col, props in table_conn.schema["partition_keys"].items():
            partition_columns[col] = props["type"]

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
                storage_options=table_conn.options,
            )
            # Adds partition values as columns
            f_partitions = partitions_in_file(f)
            for k, v in f_partitions.items():
                casting_func = casting_functions(
                    table_conn.schema["partition_keys"][k]["type"]
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
    def __parse_identifier_list(
        cls, tokens: list[Token], alias_map: dict[str, str]
    ) -> dict[str, dict[str, str]]:
        identifier_list = [t for t in tokens if type(t) == IdentifierList][0]
        return identifierlist2dict(identifier_list, alias_map)

    @classmethod
    def __parse_identifier_aliases(cls, tokens: list[Token]) -> dict[str, str]:
        identifier_list = [t for t in tokens if type(t) == Identifier]
        return aliases2dict(identifier_list)

    @classmethod
    def __parse_inner_join_mappings(
        cls, tokens: list[Token], alias_map: dict[str, str]
    ) -> dict[str, str]:
        join_mappings = [t for t in tokens if type(t) == Comparison]
        return join_comparison_mapping(join_mappings, alias_map)

    @classmethod
    def __parse_filters_for_reading(
        cls, tokens: list[Token], alias_map: dict[str, str]
    ) -> list[tuple[str, ReadingFilter]]:
        """
        Filters the filters that might help deciding which
        partition files must be read, optimizing reading time.

        Filters that are considered:

        - Column equality to constant
        - Column unequality to constant
        - Column belonging to set
        - Column not belonging to set

        """
        filters = [t for t in tokens if type(t) == Where]
        # return filters
        if len(filters) > 0:
            return where2filtermap(filters[0].tokens)
        else:
            return []

    @classmethod
    def __parse_filters_for_query(
        cls, tokens: list[Token], alias_map: dict[str, str]
    ) -> str | None:
        filters = [t for t in tokens if type(t) == Where]
        if len(filters) > 0:
            return where2pandas(filters[0].tokens, alias_map)
        else:
            return None

    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection):
        table_aliases = cls.__parse_identifier_aliases(tokens)
        column_identifiers = cls.__parse_identifier_list(tokens, table_aliases)
        joins = cls.__parse_inner_join_mappings(tokens, table_aliases)
        reading_filters = cls.__parse_filters_for_reading(
            tokens, table_aliases
        )
        where_filters = cls.__parse_filters_for_query(tokens, table_aliases)
        dfs: dict[str, pd.DataFrame] = {}
        for alias, name in table_aliases.items():
            dfs[alias] = cls.__parse_select_from_table(
                name,
                [f[1] for f in reading_filters if f[0] == alias],
                column_identifiers.get(alias, {}),
                conn,
            )
        # TODO - Process joins in order
        if len(joins) > 0:
            pass
        else:
            df: pd.DataFrame = dfs[list(table_aliases.keys())[0]]
        if where_filters is not None:
            df = df.query(where_filters)
        return df


MAPPING: dict[str, type[QueryParser]] = {
    "CREATE": CREATEParser,
    "ALTER": ALTERParser,
    "DROP": DROPParser,
    "INSERT": INSERTParser,
    "UPDATE": UPDATEParser,
    "DELETE": DELETEParser,
    "SELECT": SELECTParser,
}


def factory(kind: str) -> type[QueryParser]:
    return MAPPING[kind]
