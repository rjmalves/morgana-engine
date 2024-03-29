from abc import ABC
import json
from os import listdir
from os.path import join
import s3fs  # type: ignore
import pathlib

from morgana_engine.models.schema import Schema
from morgana_engine.utils.uri import (
    is_uri,
    uri_scheme,
    ensure_absolute_path,
    path_to_uri,
    uri_to_path,
)


class Connection(ABC):
    """
    Class that wraps a database connection with the required connection
    options depending on the data storage that is selected.

    The connection is uniquely identified by an URI, which points to a
    directory that has a schema, which has a syntax that is derived from
    JSON schema standards, with specialized fields.
    """

    def __init__(self, *args, **kwargs) -> None:
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

    def __init__(self, uri: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if is_uri(uri):
            self._uri = uri
        else:
            self._uri = path_to_uri(ensure_absolute_path(uri, "."), "file")

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def path(self) -> str:
        return uri_to_path(self._uri)

    @property
    def storage_options(self) -> dict:
        return {}

    @property
    def schema(self) -> Schema:
        if self._schema is None:
            with open(join(self.path, "schema.json"), "r") as file:
                self._schema = Schema(json.load(file))
        return self._schema

    def list_files(self) -> list[str]:
        if not self.schema.is_table:
            raise ValueError("Cannot list files from a database schema")
        files_with_extension = listdir(self.path)
        return [f.split(".")[0] for f in files_with_extension]

    def list_partition_files(self, column: str) -> list[str]:
        files = self.list_files()
        # TODO - replace simple comparison by regex
        # maybe this comparison is not needed, all files will
        # belong to the table
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
            table_uri = tables[table_name]
            # Field id already an URI: just access
            if is_uri(table_uri):
                if uri_scheme(table_uri).lower() == "file":
                    return FSConnection(
                        table_uri, storage_options=self.storage_options
                    )
                else:
                    raise ValueError(
                        f"Table URI {table_uri} is not a valid file URI"
                    )
            else:
                # Field is a path: convert to URI and access
                table_path = ensure_absolute_path(table_uri, self.path)
                return FSConnection(
                    path_to_uri(table_path, "file"),
                    storage_options=self.storage_options,
                )
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
    Class that wraps a database connection to a S3 bucket, providing
    to the user the abstraction for vieweing the FS as a database.

    The S3 authentication is made via `s3fs` library, which expects
    given arguments for the authentication process. See:

    https://s3fs.readthedocs.io/en/latest/

    These arguments must be passed to the constructor via `storage_options`,
    as a dictionary. For example:

    ```
    storage_options = {
        key: "my-key",
        secret: "my-secret",
    }

    conn = S3Connection("s3://my-bucket", storage_options=storage_options)
    ```

    """

    def __init__(self, uri: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if is_uri(uri):
            self._uri = uri
        else:
            self._uri = path_to_uri(uri, "s3")

        if "storage_options" in kwargs:
            self._storage_options = kwargs["storage_options"]
        else:
            self._storage_options = {}
        self._s3 = s3fs.S3FileSystem(**self._storage_options)

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def path(self) -> str:
        return uri_to_path(self._uri)

    @property
    def storage_options(self) -> dict:
        return self._storage_options

    @property
    def schema(self) -> Schema:
        if self._schema is None:
            with self._s3.open(join(self.path, "schema.json"), "r") as file:
                self._schema = Schema(json.load(file))
        return self._schema

    def list_files(self) -> list[str]:
        if not self.schema.is_table:
            raise ValueError("Cannot list files from a database schema")
        files_with_extension = self._s3.ls(self.uri)
        filenames_with_extension = [
            pathlib.Path(f).parts[-1] for f in files_with_extension
        ]
        return [f.split(".")[0] for f in filenames_with_extension]

    def list_partition_files(self, column: str) -> list[str]:
        files = self.list_files()
        # TODO - replace simple comparison by regex
        # maybe this comparison is not needed, all files will
        # belong to the table
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
            table_uri = tables[table_name]
            # Field id already an URI: just access
            if is_uri(table_uri):
                if uri_scheme(table_uri).lower() == "s3":
                    return S3Connection(
                        table_uri, storage_options=self.storage_options
                    )
                else:
                    raise ValueError(
                        f"Table URI {table_uri} is not a valid S3 URI"
                    )
            else:
                # Field is a path: convert to URI and access
                table_path = join(self.path, table_uri)
                return S3Connection(
                    path_to_uri(table_path, "s3"),
                    storage_options=self.storage_options,
                )
        else:
            raise ValueError(f"Table {table_name} not found!")


MAPPING: dict[str, type[Connection]] = {
    "FS": FSConnection,
    "SQL": SQLConnection,
    "S3": S3Connection,
}


def factory(kind: str) -> type[Connection]:
    return MAPPING.get(kind, S3Connection)
