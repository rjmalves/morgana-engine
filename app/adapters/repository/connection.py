from abc import ABC, abstractmethod
from typing import override
import json
from os.path import join
from app.utils.types import enforce_property_types


class Connection(ABC):
    def __init__(self) -> None:
        self._schema: dict | None = None

    @property
    @abstractmethod
    def uri(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def options(self) -> dict:
        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> dict:
        raise NotImplementedError

    @abstractmethod
    def access(self, table: str) -> "Connection":
        raise NotImplementedError



class FSConnection(Connection):
    def __init__(self, path: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._path = path

    @override
    @property
    def uri(self) -> str:
        return self._path

    @override
    @property
    def options(self) -> dict:
        return {}

    @override
    @property
    def schema(self) -> dict:
        if self._schema is None:
            with open(join(self.uri, ".schema.json"), "r") as file:
                self._schema = json.load(file)
                enforce_property_types(self._schema)
        return self._schema

    @override
    def access(self, table: str) -> "Connection":
        tables = self.schema["properties"]
        if table in tables:
            return FSConnection(join(self.uri, tables[table]["$ref"]))
        else:
            # TODO - add error handling
            return self



class SQLConnection(Connection):
    pass


class S3Connection(Connection):
    pass


MAPPING: dict[str, type[Connection]] = {
    "FS": FSConnection,
    "SQL": SQLConnection,
    "S3": S3Connection,
}


def factory(kind: str) -> type[Connection]:
    return MAPPING.get(kind, S3Connection)
