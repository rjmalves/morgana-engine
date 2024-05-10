from morgana_engine.models.sql import (
    SQLTokenType,
    SQLToken,
    SQLStatement,
    SQLParser,
    ParsingResult,
)


from morgana_engine.adapters.repository.connection import Connection
from morgana_engine.models.parsedsql import Column, Table, QueryingFilter
from typing import Optional, List


class SELECTParser(SQLParser):

    @staticmethod
    def match_statement(statement: SQLStatement) -> bool:
        return statement.tokens[0].type == SQLTokenType.SELECT

    def __validate_select_from(self) -> Optional[ParsingResult]:
        tokens = self.statement.tokens
        select_tokens = list(
            filter(lambda t: t.type == SQLTokenType.SELECT, tokens)
        )
        if len(select_tokens) != 1:
            return ParsingResult(
                status=False,
                message="The statement must contain 1 SELECT",
            )
        from_tokens = list(
            filter(lambda t: t.type == SQLTokenType.FROM, tokens)
        )
        if len(from_tokens) != 1:
            return ParsingResult(
                status=False,
                message="The statement must contain FROM",
            )
        self.__select_index = tokens.index(select_tokens[0])
        self.__from_index = tokens.index(from_tokens[0])
        last_index = len(tokens)
        if self.__from_index - self.__select_index < 2:
            return ParsingResult(
                status=False,
                message="The statement must contain at least"
                + " one entity between SELECT and FROM",
            )
        if last_index - self.__from_index < 2:
            return ParsingResult(
                status=False,
                message="The statement must contain at least"
                + " one entity after FROM",
            )
        return None

    def __validate_where(self) -> Optional[ParsingResult]:
        tokens = self.statement.tokens
        where_tokens = list(
            filter(lambda t: t.type == SQLTokenType.WHERE, tokens)
        )
        if len(where_tokens) > 1:
            return ParsingResult(
                status=False,
                message="The statement may contain 1 WHERE",
            )
        self.__filtered = len(where_tokens) == 1
        self.__where_index = (
            tokens.index(where_tokens[0]) if self.__filtered else None
        )
        if self.__filtered:
            last_index = len(tokens)
            if last_index - self.__where_index < 2:
                return ParsingResult(
                    status=False,
                    message="The statement must contain at least"
                    + " one entity after WHERE",
                )
        return None

    @staticmethod
    def __split_by_token_type(
        tokens: List[SQLToken], token_type: SQLTokenType
    ) -> List[List[SQLToken]]:
        tokens_of_type = list(filter(lambda t: t.type == token_type, tokens))
        tokens_indices = (
            [-1] + [tokens.index(c) for c in tokens_of_type] + [len(tokens)]
        )
        splitting_indices = []
        for i in range(len(tokens_indices) - 1):
            splitting_indices.append(
                (tokens_indices[i] + 1, tokens_indices[i + 1])
            )
        return [tokens[s:e] for s, e in splitting_indices]

    def __get_querying_tables(self) -> Optional[ParsingResult]:

        self.tables: List[Table] = []
        # Gets tokens between FROM and WHERE (or the end)
        # for considering as columns
        last_index = (
            self.__where_index
            if self.__filtered
            else len(self.statement.tokens)
        )
        tokens = self.statement.tokens[self.__from_index + 1 : last_index]

        table_tokens = self.__split_by_token_type(tokens, SQLTokenType.COMMA)
        for column_token_group in table_tokens:
            aliases_tokens = self.__split_by_token_type(
                column_token_group, SQLTokenType.AS
            )
            table_name = aliases_tokens[0][0].text
            table_alias = (
                aliases_tokens[-1][0].text if len(aliases_tokens) > 1 else None
            )

            self.tables.append(
                Table(name=table_name, alias=table_alias, columns=[])
            )

        # TODO - maybe needs to parse for "ON" and "JOIN" tokens
        # for supporting the case where the tables don't have
        # aliases
        return None

    def __get_schema_elements(self) -> Optional[ParsingResult]:
        pass

    def __get_querying_columns(self) -> Optional[ParsingResult]:

        self.columns: List[Column] = []
        # Gets tokens between SELECT and FROM
        # for considering as columns
        tokens = self.statement.tokens[
            self.__select_index + 1 : self.__from_index
        ]
        # Splits the tokens by commas
        # into sublists for each column,
        # possibly with aliases
        column_tokens = self.__split_by_token_type(tokens, SQLTokenType.COMMA)
        for column_token_group in column_tokens:
            aliases_tokens = self.__split_by_token_type(
                column_token_group, SQLTokenType.AS
            )
            column_alias = (
                aliases_tokens[-1][0].text if len(aliases_tokens) > 1 else None
            )
            columns_with_table = self.__split_by_token_type(
                aliases_tokens[0], SQLTokenType.DOT
            )
            if len(columns_with_table) > 1:
                table_name = columns_with_table[0][0].text
                column_name = columns_with_table[1][0].text
            else:
                table_name = None
                column_name = columns_with_table[0][0].text

            self.columns.append(
                Column(
                    name=column_name,
                    alias=column_alias,
                )
            )
            # print(table_name, column_name, column_alias)

        return None

    def __validate_tables(self) -> Optional[ParsingResult]:
        pass

    def __validate_columns(self) -> Optional[ParsingResult]:
        pass

    def __validate_filters(self) -> Optional[ParsingResult]:
        pass

    def validate(self) -> Optional[ParsingResult]:
        # Assert exists SELECT and FROM, with at least one entity
        # after each
        self.__validate_select_from()
        # Validates if the WHERE condition containts at
        # least one comparison
        self.__validate_where()
        # ...
        # Builds the AST of the elements
        self.__get_querying_tables()
        # self.__get_schema_elements()
        # self.__get_querying_columns()
        # Checks if all the tables exist
        self.__validate_tables()
        # Checks if all the columns from each table
        # exist
        self.__validate_columns()
        # Checks all the filters that are applied for
        # typing and existence
        self.__validate_filters()

    def parse(self) -> ParsingResult:
        pass
