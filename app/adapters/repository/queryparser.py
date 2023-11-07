from abc import ABC
from app.adapters.repository.connection import Connection
from app.adapters.repository.dataio import DataIO
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
from app.utils.sql import identifierlist2dict, aliases2dict, where2pandas


class QueryParser(ABC):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class CREATEParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class ALTERParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class DROPParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class INSERTParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class UPDATEParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class DELETEParser(QueryParser):
    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        raise NotImplementedError


class SELECTParser(QueryParser):
    @classmethod
    def __parse_select_from_table(
        cls, table: str, columns: list[str], conn: Connection, dataio: DataIO
    ):
        pass

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
    def __parse_join_mappings(cls, tokens: list[Token]) -> dict[str, str]:
        pass

    @classmethod
    def __parse_filters(
        cls, tokens: list[Token], alias_map: dict[str, str]
    ) -> str | None:
        filters = [t for t in tokens if type(t) == Where]
        if len(filters) > 0:
            return where2pandas(filters, alias_map)
        else:
            return None

    @classmethod
    def parse(cls, tokens: list[Token], conn: Connection, dataio: DataIO):
        identifiers = cls.__parse_identifier_list(tokens)
        aliases = cls.__parse_identifier_aliases(tokens)
        filters = cls.__parse_filters(tokens, aliases)
        dfs: dict[str, pd.DataFrame] = []
        for alias, name in aliases.items():
            dfs[alias] = cls.__parse_select_from_table(
                name, identifiers.get(alias, []), conn, dataio
            )
        # TODO - Process joins in order
        df: pd.DataFrame = None
        if filters is not None:
            df = df.query(filters)
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
