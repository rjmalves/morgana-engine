from abc import ABC
import json
from os import listdir
from os.path import join

from app.models.schema import Schema


class Connection(ABC):
    """
    Class that wraps a database connection with the required connection
    options depending on the data storage that is selected.

    The connection is uniquely identified by an URI, which points to a
    directory that has a schema, which has a syntax that is derived from
    JSON schema standards, with specialized fields.
    """

    def __init__(self) -> None:
        self._schema: Schema | None = None

    @property
    def uri(self) -> str:
        """
        The unique identifier of the DB connection, used to locate the data.
        """
        raise NotImplementedError

    @property
    def storage_options(self) -> dict:
        """
        The connection options that must be passed to the IO functions in order
        to interact with the database.
        """
        raise NotImplementedError

    @property
    def schema(self) -> Schema:
        """
        The database or table schema for describing the associated data.
        """
        raise NotImplementedError

    def list_files(self) -> list[str]:
        """
        Lists the files that are available for reading in the connection's URI.
        """
        raise NotImplementedError

    def list_partition_files(self, column: str) -> list[str]:
        """
        Lists the files that are available for reading in the connection's URI and
        partition the data according to a given column.
        """
        raise NotImplementedError

    def access(self, table: str) -> "Connection":
        """
        Constructs another connection object for handling access to a given table, when
        the current connection is associated to a database schema.
        """
        raise NotImplementedError


class FSConnection(Connection):
    """
    Class that wraps a database connection to the local FileSystem, providing
    to the user the abstraction for vieweing the FS as a database.
    """

    def __init__(self, path: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._path = path

    @property
    def uri(self) -> str:
        return self._path

    @property
    def storage_options(self) -> dict:
        return {}

    @property
    def schema(self) -> Schema:
        if self._schema is None:
            with open(join(self.uri, ".schema.json"), "r") as file:
                self._schema = Schema(json.load(file))
        return self._schema

    def list_files(self) -> list[str]:
        if not self.schema.is_table:
            raise ValueError("Cannot list files from a database schema")
        files_with_extension = listdir(self.uri)
        return [f.split(".")[0] for f in files_with_extension]

    def list_partition_files(self, column: str) -> list[str]:
        files = self.list_files()
        # TODO - replace simple comparison by regex
        files_with_column = [f for f in files if f"-{column}" in f]
        return files_with_column

    def access(self, table_name: str) -> "Connection":
        if not self.schema.is_database:
            raise ValueError(
                f"Schema {self.uri} is not associated with a database"
            )
        tables = self.schema.tables
        if tables is None:
            raise ValueError("Schema does not have any tables")
        elif table_name in tables:
            return FSConnection(join(self.uri, tables[table_name]))
        else:
            raise ValueError(f"Table {table_name} not found!")


class SQLConnection(Connection):
    """
    TODO - is it possible to wrap a SQL database connection, transcribing
    the required schema fields to specific DB-SQL table metadata commands?
    """

    pass


class S3Connection(Connection):
    """
    TODO - implement S3 connection using s3fs
    """

    pass


MAPPING: dict[str, type[Connection]] = {
    "FS": FSConnection,
    "SQL": SQLConnection,
    "S3": S3Connection,
}


def factory(kind: str) -> type[Connection]:
    return MAPPING.get(kind, S3Connection)
