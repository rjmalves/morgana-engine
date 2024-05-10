from morgana_engine.models.sql import (
    SQLStatement,
    ParsingResult,
    SQLParser,
)
from typing import List, Type

from morgana_engine.services.interpreters.parsers.select import SELECTParser
from morgana_engine.adapters.repository.connection import Connection

PARSERS: List[Type[SQLParser]] = [SELECTParser]


def _factory(
    statement: SQLStatement,
) -> Type[SQLParser]:
    for p in PARSERS:
        if p.match_statement(statement):
            return p
    else:
        raise NotImplementedError(
            f"Statement type {statement.tokens[0].type} not implemented"
        )


def parse(statement: SQLStatement, conn: Connection) -> ParsingResult:
    print("HERE")
    parser_type = _factory(statement)
    parser = parser_type(statement, conn)
    validation_result = parser.validate()
    # if validation_result:
    #     return validation_result
    # else:
    #     return parser.parse()
