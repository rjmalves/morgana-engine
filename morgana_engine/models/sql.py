from enum import Enum
from typing import Optional, List
from dataclasses import dataclass
import pandas as pd  # type: ignore
from morgana_engine.adapters.repository.connection import Connection


class SQLTokenType(Enum):
    SELECT = "SELECT"
    AS = "AS"
    FROM = "FROM"
    WHERE = "WHERE"
    ORDER = "ORDER"
    GROUP = "GROUP"
    BY = "BY"
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    COLUMN = "COLUMN"
    CREATE = "CREATE"
    ALTER = "ALTER"
    TABLE = "TABLE"
    INSERT = "INSERT"
    DROP = "DROP"
    DELETE = "DELETE"
    INTO = "INTO"
    VALUE = "VALUE"
    ASC = "ASC"
    DESC = "DESC"
    JOIN = "JOIN"
    INNER = "INNER"
    OUTER = "OUTER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    UPDATE = "UPDATE"
    RENAME = "RENAME"
    WILDCARD = "*"
    IN = "IN"
    NOT = "NOT"
    NOT_IN = "NOT IN"
    AND = "AND"
    OR = "OR"
    ON = "ON"
    # Common punctuation tokens
    DOT = "."
    COMMA = ","
    SEMICOLON = ";"
    LPAREN = "("
    RPAREN = ")"
    EQUALS = "="
    DIFFERENT = "!="
    GREATER = ">"
    GREATER_EQUAL = ">="
    LESS = "<"
    LESS_EQUAL = "<="
    # Custom
    ENTITY = None

    @classmethod
    def factory(cls, val: str) -> Optional["SQLTokenType"]:
        for member in cls:
            if member.value == val:
                return member
        return None


PUNCTUATION_TOKEN_TYPES = [
    SQLTokenType.DOT,
    SQLTokenType.COMMA,
    SQLTokenType.SEMICOLON,
    SQLTokenType.LPAREN,
    SQLTokenType.RPAREN,
    SQLTokenType.EQUALS,
    SQLTokenType.GREATER,
    SQLTokenType.GREATER_EQUAL,
    SQLTokenType.LESS,
    SQLTokenType.LESS_EQUAL,
]

STATEMENT_TOKEN_TYPES = [
    SQLTokenType.SELECT,
    SQLTokenType.CREATE,
    SQLTokenType.ALTER,
    SQLTokenType.INSERT,
    SQLTokenType.UPDATE,
    SQLTokenType.RENAME,
    SQLTokenType.DROP,
]

OPERATION_TOKEN_TYPES = [
    SQLTokenType.IN,
    SQLTokenType.NOT,
    SQLTokenType.LPAREN,
    SQLTokenType.RPAREN,
    SQLTokenType.EQUALS,
    SQLTokenType.DIFFERENT,
    SQLTokenType.GREATER,
    SQLTokenType.GREATER_EQUAL,
    SQLTokenType.LESS,
    SQLTokenType.LESS_EQUAL,
]


class SQLToken:
    def __init__(self, type: SQLTokenType, text: str):
        self.type = type
        self.text = text

    def __str__(self):
        return f"`{self.text}`"

    @classmethod
    def factory(self, value: str) -> Optional["SQLToken"]:
        token_type = SQLTokenType.factory(value.upper())
        if token_type is None:
            return None
        return SQLToken(token_type, value)


class SQLStatement:

    def __init__(self, tokens: List[SQLToken]) -> None:
        self.tokens = tokens

    def __str__(self):
        elems = [str((str(token), str(token.type))) for token in self.tokens]
        return " ".join(elems)


@dataclass
class ParsingResult:
    status: bool
    message: str
    data: Optional[pd.DataFrame]


class SQLParser:

    def __init__(self, statement: SQLStatement, conn: Connection) -> None:
        self.statement = statement
        self.conn = conn

    @staticmethod
    def match_statement(statement: SQLStatement) -> bool:
        raise NotImplementedError("ABC method")

    def validate(self) -> Optional[ParsingResult]:
        raise NotImplementedError("ABC method")

    def parse(self) -> ParsingResult:
        raise NotImplementedError("ABC method")
