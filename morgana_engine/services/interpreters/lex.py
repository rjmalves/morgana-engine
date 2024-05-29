from morgana_engine.models.sql import SQLTokenType, SQLToken, SQLStatement
from morgana_engine.models.sql import (
    PUNCTUATION_TOKEN_TYPES,
)
from typing import List, Optional


class SQLLexer:
    @staticmethod
    def _contains_punctuation_token(value: str) -> Optional[SQLTokenType]:
        for punctuation_token in PUNCTUATION_TOKEN_TYPES:
            if punctuation_token.value in value:
                return punctuation_token
        return None

    @staticmethod
    def _recursive_lex(query: str) -> List[SQLToken]:
        parts = [q_part for q_part in query.split(" ") if len(q_part) > 0]
        result = []
        for part in parts:
            token = SQLToken.factory(part)
            if token:
                result.append(token)
            else:
                # Searches for punctuation inside part
                punctuation_token = SQLLexer._contains_punctuation_token(part)
                if punctuation_token:
                    value = punctuation_token.value
                    new_query = part.replace(value, f" {value} ")
                    result += SQLLexer._recursive_lex(new_query)
                else:
                    result.append(SQLToken(SQLTokenType.ENTITY, part))
        return result


def lex(query: str) -> SQLStatement:
    q = query.strip().replace("\n", "")
    return SQLStatement(SQLLexer._recursive_lex(q))
