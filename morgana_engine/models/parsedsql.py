from dataclasses import dataclass


@dataclass
class Column:
    """
    Class for representing a column in a SQL query.
    """

    __slots__ = [
        "name",
        "alias",
        "type_str",
        "table_name",
        "table_alias",
        "has_parent_in_token",
        "partition",
    ]

    name: str
    alias: str | None
    type_str: str | None
    table_name: str
    table_alias: str | None
    has_parent_in_token: bool
    partition: bool

    @property
    def fullname(self) -> str:
        if self.alias:
            fullname = self.alias
        elif not self.alias and self.table_alias and self.has_parent_in_token:
            fullname = f"{self.name}_{self.table_alias}"
        elif self.has_parent_in_token:
            fullname = f"{self.name}_{self.table_name}"
        else:
            fullname = self.name
        return fullname


@dataclass
class Table:
    """
    Class for representing a table in a SQL query.
    """

    __slots__ = ["name", "alias", "columns"]

    name: str
    alias: str | None
    columns: list[Column]


@dataclass
class QueryingFilter:
    """
    Class for representing a filter in a SQL query.
    """

    __slots__ = ["column", "operator", "value"]

    column: Column
    operator: str
    value: str

    @property
    def is_collection(self):
        return "(" in self.value and ")" in self.value

    def __repr__(self) -> str:
        return f"{self.column.fullname} {self.operator} {self.value}"
