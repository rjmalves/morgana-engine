from morgana_engine.models.readingfilter import (
    ReadingFilter,
    EqualityReadingFilter,
    UnequalityReadingFilter,
    InSetReadingFilter,
    NotInSetReadingFilter,
)
from morgana_engine.models.parsedsql import Column
from morgana_engine.models.sql import SQLToken, SQLTokenType
import pytest


class TestReadingFilter:
    column = Column(
        name="colname",
        alias=None,
        type_str=None,
        table_name="table",
        table_alias=None,
        has_parent_in_token=False,
        partition=False,
        querying=True,
    )

    def test_eq(self):
        filter1 = ReadingFilter(
            TestReadingFilter.column,
            SQLToken(SQLTokenType.EQUALS, "="),
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        filter2 = ReadingFilter(
            TestReadingFilter.column,
            SQLToken(SQLTokenType.LESS, "<"),
            [SQLToken(SQLTokenType.ENTITY, "5")],
        )
        assert filter1 == filter1
        assert filter1 != filter2

    def test_is_filter(self):
        with pytest.raises(NotImplementedError):
            assert ReadingFilter.is_filter(SQLToken(SQLTokenType.EQUALS, "="))

    def test_apply(self):
        with pytest.raises(NotImplementedError):
            filter = ReadingFilter(
                TestReadingFilter.column,
                SQLToken(SQLTokenType.EQUALS, "="),
                [SQLToken(SQLTokenType.ENTITY, "10")],
            )
            values = [5, 10, 15, 20]
            casting_func = int
            filter.apply(values, casting_func)


class TestEqualityReadingFilter:
    equal_token = SQLToken(SQLTokenType.EQUALS, "=")
    diff_token = SQLToken(SQLTokenType.DIFFERENT, "!=")
    column = Column(
        name="colname",
        alias=None,
        type_str="int",
        table_name="table",
        table_alias=None,
        has_parent_in_token=False,
        partition=False,
        querying=True,
    )

    def test_is_filter(self):
        assert EqualityReadingFilter.is_filter(
            TestEqualityReadingFilter.equal_token
        )
        assert EqualityReadingFilter.is_filter(
            TestEqualityReadingFilter.diff_token
        )

    def test_operators(self):
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.column,
            TestEqualityReadingFilter.equal_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        assert (
            filter.operator.type == TestEqualityReadingFilter.equal_token.type
        )
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.column,
            TestEqualityReadingFilter.diff_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        assert filter.operator.type == TestEqualityReadingFilter.diff_token.type

    def test_values(self):
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.column,
            TestEqualityReadingFilter.equal_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        assert filter.values == ["10"]

    def test_apply(self):
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.column,
            TestEqualityReadingFilter.equal_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [10]
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.column,
            TestEqualityReadingFilter.diff_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [5, 15, 20]


class TestUnequalityReadingFilter:
    gt_token = SQLToken(SQLTokenType.GREATER, ">")
    le_token = SQLToken(SQLTokenType.LESS_EQUAL, "<=")
    column = Column(
        name="colname",
        alias=None,
        type_str="int",
        table_name="table",
        table_alias=None,
        has_parent_in_token=False,
        partition=False,
        querying=True,
    )

    def test_is_filter(self):
        assert UnequalityReadingFilter.is_filter(
            TestUnequalityReadingFilter.gt_token
        )
        assert UnequalityReadingFilter.is_filter(
            TestUnequalityReadingFilter.le_token
        )

    def test_operators(self):
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.column,
            TestUnequalityReadingFilter.gt_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        assert filter.operator.type == TestUnequalityReadingFilter.gt_token.type
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.column,
            TestUnequalityReadingFilter.le_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        assert filter.operator.type == TestUnequalityReadingFilter.le_token.type

    def test_values(self):
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.column,
            TestUnequalityReadingFilter.gt_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        assert filter.values == ["10"]

    def test_apply(self):
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.column,
            TestUnequalityReadingFilter.gt_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [15, 20]
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.column,
            TestUnequalityReadingFilter.le_token,
            [SQLToken(SQLTokenType.ENTITY, "10")],
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [5, 10]


class TestInSetReadingFilter:
    in_token = SQLToken(SQLTokenType.IN, "IN")
    column = Column(
        name="colname",
        alias=None,
        type_str="int",
        table_name="table",
        table_alias=None,
        has_parent_in_token=False,
        partition=False,
        querying=True,
    )

    def test_is_filter(self):
        assert InSetReadingFilter.is_filter(TestInSetReadingFilter.in_token)

    def test_operators(self):
        filter = InSetReadingFilter(
            TestInSetReadingFilter.column,
            TestInSetReadingFilter.in_token,
            [
                SQLToken(SQLTokenType.ENTITY, "10"),
                SQLToken(SQLTokenType.ENTITY, "15"),
            ],
        )
        assert filter.operator.type == TestInSetReadingFilter.in_token.type

    def test_values(self):
        filter = InSetReadingFilter(
            TestInSetReadingFilter.column,
            TestInSetReadingFilter.in_token,
            [
                SQLToken(SQLTokenType.ENTITY, "10"),
                SQLToken(SQLTokenType.ENTITY, "15"),
            ],
        )
        assert filter.values == ["10", "15"]

    def test_apply(self):
        filter = InSetReadingFilter(
            TestInSetReadingFilter.column,
            TestInSetReadingFilter.in_token,
            [
                SQLToken(SQLTokenType.ENTITY, "10"),
                SQLToken(SQLTokenType.ENTITY, "15"),
            ],
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [10, 15]


class TestNotInSetReadingFilter:
    not_in_token = SQLToken(SQLTokenType.NOT_IN, "NOT IN")
    column = Column(
        name="colname",
        alias=None,
        type_str="int",
        table_name="table",
        table_alias=None,
        has_parent_in_token=False,
        partition=False,
        querying=True,
    )

    def test_is_filter(self):
        assert NotInSetReadingFilter.is_filter(
            TestNotInSetReadingFilter.not_in_token
        )

    def test_operators(self):
        filter = NotInSetReadingFilter(
            TestNotInSetReadingFilter.column,
            TestNotInSetReadingFilter.not_in_token,
            [
                SQLToken(SQLTokenType.ENTITY, "10"),
                SQLToken(SQLTokenType.ENTITY, "15"),
            ],
        )
        assert (
            filter.operator.type == TestNotInSetReadingFilter.not_in_token.type
        )

    def test_values(self):
        filter = NotInSetReadingFilter(
            TestNotInSetReadingFilter.column,
            TestNotInSetReadingFilter.not_in_token,
            [
                SQLToken(SQLTokenType.ENTITY, "10"),
                SQLToken(SQLTokenType.ENTITY, "15"),
            ],
        )
        assert filter.values == ["10", "15"]

    def test_apply(self):
        filter = NotInSetReadingFilter(
            TestNotInSetReadingFilter.column,
            TestNotInSetReadingFilter.not_in_token,
            [
                SQLToken(SQLTokenType.ENTITY, "10"),
                SQLToken(SQLTokenType.ENTITY, "15"),
            ],
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [5, 20]
