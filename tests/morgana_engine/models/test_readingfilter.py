from morgana_engine.models.readingfilter import (
    ReadingFilter,
    EqualityReadingFilter,
    UnequalityReadingFilter,
    InSetReadingFilter,
    NotInSetReadingFilter,
)
from morgana_engine.models.parsedsql import Column, Table
from morgana_engine.utils.sql import query2tokens, filter_spacing_tokens
import pytest


class TestReadingFilter:
    tokens = query2tokens("SELECT * FROM table WHERE colname > 10")
    parsed_tokens = filter_spacing_tokens(tokens[-1].tokens[1:])[0].tokens
    table = Table(
        name="table",
        alias=None,
        columns=[
            Column(
                name="colname",
                alias=None,
                type_str=None,
                table_name="table",
                table_alias=None,
                has_parent_in_token=False,
                partition=False,
            )
        ],
    )

    def test_column(self):
        filter = ReadingFilter(TestReadingFilter.parsed_tokens, self.table)
        assert filter.column == Column(
            name="colname",
            alias=None,
            type_str=None,
            table_name="table",
            table_alias=None,
            has_parent_in_token=False,
            partition=False,
        )

    def test_eq(self):
        with pytest.raises(NotImplementedError):
            tokens2 = query2tokens("SELECT * FROM table WHERE colname < 10")
            parsed_tokens2 = filter_spacing_tokens(tokens2[-1].tokens[1:])[
                0
            ].tokens
            filter1 = ReadingFilter(
                TestReadingFilter.parsed_tokens, self.table
            )
            tokens2 = query2tokens("SELECT * FROM table WHERE column < 5")
            filter2 = ReadingFilter(
                parsed_tokens2,
                Table(
                    name="table",
                    alias=None,
                    columns=[
                        Column(
                            name="column",
                            alias=None,
                            type_str=None,
                            table_name="table",
                            table_alias=None,
                            has_parent_in_token=False,
                            partition=False,
                        )
                    ],
                ),
            )
            assert filter1 == filter1
            assert filter1 != filter2

    def test_is_filter(self):
        with pytest.raises(NotImplementedError):
            assert ReadingFilter.is_filter(TestReadingFilter.parsed_tokens)

    def test_apply(self):
        with pytest.raises(NotImplementedError):
            filter = ReadingFilter(TestReadingFilter.parsed_tokens, self.table)
            values = [5, 10, 15, 20]
            casting_func = int
            filter.apply(values, casting_func)


class TestEqualityReadingFilter:
    equal_tokens = query2tokens("SELECT * FROM table WHERE colname = 10")
    parsed_equal_tokens = filter_spacing_tokens(equal_tokens[-1].tokens[1:])[
        0
    ].tokens
    diff_tokens = query2tokens("SELECT * FROM table WHERE colname != 10")
    parsed_diff_tokens = filter_spacing_tokens(diff_tokens[-1].tokens[1:])[
        0
    ].tokens
    table = Table(
        name="table",
        alias=None,
        columns=[
            Column(
                name="colname",
                alias=None,
                type_str=None,
                table_name="table",
                table_alias=None,
                has_parent_in_token=False,
                partition=False,
            )
        ],
    )

    def test_is_filter(self):
        assert EqualityReadingFilter.is_filter(
            TestEqualityReadingFilter.parsed_equal_tokens
        )
        assert EqualityReadingFilter.is_filter(
            TestEqualityReadingFilter.parsed_diff_tokens
        )

    def test_operators(self):
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.parsed_equal_tokens,
            TestEqualityReadingFilter.table,
        )
        assert filter.operators == ["="]
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.parsed_diff_tokens,
            TestEqualityReadingFilter.table,
        )
        assert filter.operators == ["!="]

    def test_values(self):
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.parsed_equal_tokens,
            TestEqualityReadingFilter.table,
        )
        assert filter.values == ["10"]

    def test_apply(self):
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.parsed_equal_tokens,
            TestEqualityReadingFilter.table,
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [10]
        filter = EqualityReadingFilter(
            TestEqualityReadingFilter.parsed_diff_tokens,
            TestEqualityReadingFilter.table,
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [5, 15, 20]


class TestUnequalityReadingFilter:
    direct_gt_tokens = query2tokens("SELECT * FROM table WHERE colname > 10")
    parsed_direct_gt_tokens = filter_spacing_tokens(
        direct_gt_tokens[-1].tokens[1:]
    )[0].tokens
    reverse_le_tokens = query2tokens("SELECT * FROM table WHERE 10 <= colname")
    parsed_reverse_le_tokens = filter_spacing_tokens(
        reverse_le_tokens[-1].tokens[1:]
    )[0].tokens
    table = Table(
        name="table",
        alias=None,
        columns=[
            Column(
                name="colname",
                alias=None,
                type_str=None,
                table_name="table",
                table_alias=None,
                has_parent_in_token=False,
                partition=False,
            )
        ],
    )

    def test_is_filter(self):
        assert UnequalityReadingFilter.is_filter(
            TestUnequalityReadingFilter.parsed_direct_gt_tokens
        )
        assert UnequalityReadingFilter.is_filter(
            TestUnequalityReadingFilter.parsed_reverse_le_tokens
        )

    def test_operators(self):
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.parsed_direct_gt_tokens,
            TestUnequalityReadingFilter.table,
        )
        assert filter.operators == [">"]
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.parsed_reverse_le_tokens,
            TestUnequalityReadingFilter.table,
        )
        assert filter.operators == [">="]

    def test_values(self):
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.parsed_direct_gt_tokens,
            TestUnequalityReadingFilter.table,
        )
        assert filter.values == ["10"]

    def test_apply(self):
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.parsed_direct_gt_tokens,
            TestUnequalityReadingFilter.table,
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [15, 20]
        filter = UnequalityReadingFilter(
            TestUnequalityReadingFilter.parsed_reverse_le_tokens,
            TestUnequalityReadingFilter.table,
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [10, 15, 20]


class TestInSetReadingFilter:
    tokens = query2tokens("SELECT * FROM table WHERE colname IN (10, 15)")
    parsed_tokens = filter_spacing_tokens(tokens[-1].tokens[1:])
    table = Table(
        name="table",
        alias=None,
        columns=[
            Column(
                name="colname",
                alias=None,
                type_str=None,
                table_name="table",
                table_alias=None,
                has_parent_in_token=False,
                partition=False,
            )
        ],
    )

    def test_is_filter(self):
        assert InSetReadingFilter.is_filter(
            TestInSetReadingFilter.parsed_tokens
        )

    def test_operators(self):
        filter = InSetReadingFilter(
            TestInSetReadingFilter.parsed_tokens,
            TestInSetReadingFilter.table,
        )
        assert filter.operators == ["IN"]

    def test_values(self):
        filter = InSetReadingFilter(
            TestInSetReadingFilter.parsed_tokens,
            TestInSetReadingFilter.table,
        )
        assert filter.values == ["10", "15"]

    def test_apply(self):
        filter = InSetReadingFilter(
            TestInSetReadingFilter.parsed_tokens,
            TestInSetReadingFilter.table,
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [10, 15]


class TestNotInSetReadingFilter:
    tokens = query2tokens("SELECT * FROM table WHERE colname NOT IN (10, 15)")
    parsed_tokens = filter_spacing_tokens(tokens[-1].tokens[1:])
    table = Table(
        name="table",
        alias=None,
        columns=[
            Column(
                name="colname",
                alias=None,
                type_str=None,
                table_name="table",
                table_alias=None,
                has_parent_in_token=False,
                partition=False,
            )
        ],
    )

    def test_is_filter(self):
        assert NotInSetReadingFilter.is_filter(
            TestNotInSetReadingFilter.parsed_tokens
        )

    def test_operators(self):
        filter = NotInSetReadingFilter(
            TestNotInSetReadingFilter.parsed_tokens,
            TestNotInSetReadingFilter.table,
        )
        assert filter.operators == ["NOT", "IN"]

    def test_values(self):
        filter = NotInSetReadingFilter(
            TestNotInSetReadingFilter.parsed_tokens,
            TestNotInSetReadingFilter.table,
        )
        assert filter.values == ["10", "15"]

    def test_apply(self):
        filter = NotInSetReadingFilter(
            TestNotInSetReadingFilter.parsed_tokens,
            TestNotInSetReadingFilter.table,
        )
        values = [5, 10, 15, 20]
        casting_func = int
        assert filter.apply(values, casting_func) == [5, 20]
