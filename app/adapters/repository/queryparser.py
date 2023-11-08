from abc import ABC
from app.adapters.repository.connection import Connection
from app.adapters.repository.dataio import DataIO
from app.adapters.repository.dataio import factory as io_factory
from app.adapters.repository.partitioner import factory as part_factory
import pandas as pd
from sqlparse.sql import Token
from sqlparse.sql import (
    Parenthesis,
    Comparison,
    Where,
    Identifier,
    IdentifierList,
)
from os.path import join
from app.utils.sql import (
    identifierlist2dict,
    aliases2dict,
    join_comparison_mapping,
    where2filtermap,
    where2pandas,
)
from app.utils.types import enforce_column_types


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
        filters: list[list[Token]],
        columns: list[str],
        conn: Connection,
    ):
        table_conn = conn.access(table)
        table_io = io_factory(table_conn.schema["format"])
        # TODO - parse filters and discover which files
        # must be read based on schema definition

        # List partitioned columns from schema
        # Check which columns are partitioned by list or range

        # For each list partitioned column, check if there are filters
        # Treat the case where the filters are of equality or belonging to set (easy)
        # Treat the case where the filters are of inequality or not belonging to set (hard)

        # For each range partitioned column, check if there are filters
        # Treat the case where the filters are of equality or belonging to set (easy)
        # Treat the case where the filters are of inequality or not belonging to set (hard)

        # The main result is the list of filenames that must be read
        # and concatenated.

        df = table_io.read(
            join(table_conn.uri, table_conn.schema["data"]),
            columns=columns,
            storage_options=table_conn.options,
        )
        print(filters)
        # df = enforce_column_types(df, table_conn.schema)
        # TODO - concatenate all files that must be read
        # Rename due columns
        df.columns
        return df

    @classmethod
    def __parse_identifier_list(
        cls, tokens: list[Token]
    ) -> dict[str, list[str]]:
        identifier_list = [t for t in tokens if type(t) == IdentifierList][0]
        return identifierlist2dict(identifier_list)

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
    ) -> list[tuple[str, list[Token]]]:
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
            return where2filtermap(filters[0].tokens, alias_map)
        else:
            return None

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
        identifiers = cls.__parse_identifier_list(tokens)
        aliases = cls.__parse_identifier_aliases(tokens)
        joins = cls.__parse_inner_join_mappings(tokens, aliases)
        reading_filters = cls.__parse_filters_for_reading(tokens, aliases)
        where_filters = cls.__parse_filters_for_query(tokens, aliases)
        dfs: dict[str, pd.DataFrame] = {}
        for alias, name in aliases.items():
            dfs[alias] = cls.__parse_select_from_table(
                name,
                [f[1] for f in reading_filters if f[0] == alias],
                identifiers.get(alias, []),
                conn,
            )
        # TODO - Process joins in order
        if len(joins) > 0:
            pass
        else:
            df: pd.DataFrame = dfs[list(aliases.keys())[0]]
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
