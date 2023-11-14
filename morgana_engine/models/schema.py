class Schema:
    """
    Implements a generic schema that can describe either a database or
    a table from a database.

    Attributes:
    -----------
    uri : str
        The URI of the schema.
    name : str
        The name of the schema.
    format : str or None
        The format of the schema, if a table schema.
    is_database : bool
        True if the schema describes a database, False otherwise.
    is_table : bool
        True if the schema describes a table, False otherwise.
    tables : dict[str, str] or None
        A mapping of the tables, if the schema describes a database, with keys
        being table names and values being each table's relative or
        absolute URIs.
    columns : dict[str, str] or None
        A mapping of the columns, if the schema describes a table, with keys
        being column names and values being each column's type.
    partition_keys : dict[str, str] or None
        A mapping of the partition keys, if the schema describes a table, with
        keys being the key names and values being each key's type.
    """

    def __init__(self, json_dict: dict) -> None:
        super().__init__()
        self._json_dict = json_dict

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Schema):
            return False
        return __value._json_dict == self._json_dict

    def validate(self) -> bool:
        # TODO - implement validation (try to use jsonschema)
        # - check for required fields for every schema
        # - check for specific database fields, with table names and URI
        # uniqueness
        # - check for specific table fields, with unique column names
        # and valid types
        return True

    @property
    def uri(self) -> str:
        return self._json_dict["uri"]

    @property
    def name(self) -> str:
        return self._json_dict["name"]

    @property
    def format(self) -> str | None:
        return self._json_dict.get("format")

    @property
    def is_database(self) -> bool:
        return self._json_dict["schema_type"] == "database"

    @property
    def is_table(self) -> bool:
        return self._json_dict["schema_type"] == "table"

    @property
    def tables(self) -> dict[str, str]:
        if self.is_database:
            return {t["name"]: t["ref"] for t in self._json_dict["tables"]}
        else:
            return {}

    @property
    def columns(self) -> dict[str, str]:
        if self.is_table:
            return {c["name"]: c["type"] for c in self._json_dict["columns"]}
        else:
            return {}

    @property
    def partition_keys(self) -> dict[str, str]:
        if self.is_table:
            return {
                k["name"]: k["type"] for k in self._json_dict["partition_keys"]
            }
        else:
            return {}
