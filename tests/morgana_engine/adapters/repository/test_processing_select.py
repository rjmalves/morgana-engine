from morgana_engine.adapters.repository.processing import SELECT
from morgana_engine.utils.sql import query2tokens, filter_spacing_tokens
from morgana_engine.models.readingfilter import UnequalityReadingFilter
from morgana_engine.adapters.repository.connection import FSConnection
from morgana_engine.models.parsedsql import Column, Table, QueryingFilter
import pandas as pd
import pytz
from datetime import datetime, date


class TestSELECT:
    def test_process_tables_identifiers(self):
        query = "SELECT usinas.id, usinas.codigo, usinas.nome, usinas.capacidade_instalada FROM usinas"
        tokens = query2tokens(query)
        tables = SELECT._process_table_identifiers(tokens)
        assert tables == [Table(name="usinas", alias=None, columns=[])]

    def test_process_column_identifiers(self):
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas"
        tokens = query2tokens(query)
        conn = FSConnection("tests/data")
        tables_to_select = [Table(name="usinas", alias=None, columns=[])]
        tables_to_select = SELECT._process_column_identifiers(
            tokens, tables_to_select, conn
        )
        assert tables_to_select == [
            Table(
                name="usinas",
                alias=None,
                columns=[
                    Column(
                        name="id",
                        alias=None,
                        type_str="int",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="codigo",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="nome",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="capacidade_instalada",
                        alias=None,
                        type_str="float",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                ],
            )
        ]

    def test_process_join_mappings(self):
        query = "SELECT u.id, u.codigo, up.codigo FROM usinas u INNER JOIN usinas_part_subsis up ON u.codigo = up.codigo"
        tokens = query2tokens(query)
        tables_to_select = [
            Table(
                name="usinas",
                alias="u",
                columns=[
                    Column(
                        name="id",
                        alias=None,
                        type_str="int",
                        table_name="usinas",
                        table_alias="u",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                    Column(
                        name="codigo",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias="u",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                ],
            ),
            Table(
                name="usinas_part_subsis",
                alias="up",
                columns=[
                    Column(
                        name="codigo",
                        alias=None,
                        type_str="string",
                        table_name="usinas_part_subsis",
                        table_alias="up",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                ],
            ),
        ]
        join_map = SELECT._process_join_mappings(tokens, tables_to_select)
        assert join_map == [
            (
                Column(
                    name="codigo",
                    alias=None,
                    type_str="string",
                    table_name="usinas",
                    table_alias="u",
                    has_parent_in_token=True,
                    partition=False,
                ),
                Column(
                    name="codigo",
                    alias=None,
                    type_str="string",
                    table_name="usinas_part_subsis",
                    table_alias="up",
                    has_parent_in_token=True,
                    partition=False,
                ),
            )
        ]

    def test_process_filters_for_reading(self):
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        tokens = query2tokens(query)
        conn = FSConnection("tests/data")
        filter_tokens = filter_spacing_tokens(
            filter_spacing_tokens(tokens[-1].tokens)[-1].tokens
        )
        tables_to_select = SELECT._process_table_identifiers(tokens)
        tables_to_select = SELECT._process_column_identifiers(
            tokens, tables_to_select, conn
        )
        filters = SELECT._process_filters_for_reading(
            tokens[1:], tables_to_select
        )
        assert filters == [
            UnequalityReadingFilter(
                filter_tokens,
                tables_to_select[0],
            )
        ]

    def test_process_filters_for_querying(self):
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        tokens = query2tokens(query)
        tables_to_select = [
            Table(
                name="usinas",
                alias=None,
                columns=[
                    Column(
                        name="codigo",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="nome",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="capacidade_instalada",
                        alias=None,
                        type_str="float",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                ],
            )
        ]
        filters = SELECT._process_filters_for_querying(
            tokens, tables_to_select
        )
        assert filters == [
            QueryingFilter(tables_to_select[0].columns[-1], "  >  ", "100")
        ]

    def test_process_select_from_table(self):
        conn = FSConnection("tests/data")
        table = Table(
            name="usinas",
            alias=None,
            columns=[
                Column(
                    name="id",
                    alias=None,
                    type_str="int",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
                Column(
                    name="codigo",
                    alias=None,
                    type_str="string",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
                Column(
                    name="nome",
                    alias=None,
                    type_str="string",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
                Column(
                    name="capacidade_instalada",
                    alias=None,
                    type_str="float",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
            ],
        )

        filters = []
        columns = [
            "id",
            "codigo",
            "nome",
            "capacidade_instalada",
        ]
        process_result = SELECT._process_select_from_table(
            table, filters, conn
        )
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=columns,
        )
        assert df.equals(expected_df)

    def test_process_select(self):
        conn = FSConnection("tests/data")
        table = Table(
            name="usinas",
            alias=None,
            columns=[
                Column(
                    name="id",
                    alias=None,
                    type_str="int",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
                Column(
                    name="codigo",
                    alias=None,
                    type_str="string",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
                Column(
                    name="nome",
                    alias=None,
                    type_str="string",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
                Column(
                    name="capacidade_instalada",
                    alias=None,
                    type_str="float",
                    table_name="usinas",
                    table_alias=None,
                    has_parent_in_token=False,
                    partition=False,
                ),
            ],
        )

        filters = []
        columns = [
            "id",
            "codigo",
            "nome",
            "capacidade_instalada",
        ]
        process_result = SELECT._process_select_from_tables(
            [table], filters, conn
        )
        dfs = process_result["data"]
        assert len(dfs) == 1
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=list(columns),
        )
        assert dfs[0].equals(expected_df)

    def test_process_join_tables(self):
        conn = FSConnection("tests/data")
        tables_to_select = [
            Table(
                name="usinas",
                alias=None,
                columns=[
                    Column(
                        name="id",
                        alias=None,
                        type_str="int",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="codigo",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="nome",
                        alias=None,
                        type_str="string",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                    Column(
                        name="capacidade_instalada",
                        alias=None,
                        type_str="float",
                        table_name="usinas",
                        table_alias=None,
                        has_parent_in_token=False,
                        partition=False,
                    ),
                ],
            ),
            Table(
                name="usinas_part_subsis",
                alias="up",
                columns=[
                    Column(
                        name="id",
                        alias=None,
                        type_str="int",
                        table_name="usinas_part_subsis",
                        table_alias="up",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                    Column(
                        name="codigo",
                        alias=None,
                        type_str="string",
                        table_name="usinas_part_subsis",
                        table_alias="up",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                    Column(
                        name="nome",
                        alias=None,
                        type_str="string",
                        table_name="usinas_part_subsis",
                        table_alias="up",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                    Column(
                        name="capacidade_instalada",
                        alias=None,
                        type_str="float",
                        table_name="usinas_part_subsis",
                        table_alias="up",
                        has_parent_in_token=True,
                        partition=False,
                    ),
                ],
            ),
        ]
        filters = []
        process_result = SELECT._process_select_from_tables(
            tables_to_select, filters, conn
        )
        dfs = process_result["data"]
        df = SELECT._process_join_tables(
            dfs,
            [(tables_to_select[0].columns[0], tables_to_select[1].columns[0])],
        )
        assert df["codigo"].equals(df["codigo_up"])
        assert df["nome"].equals(df["nome_up"])
        assert df["capacidade_instalada"].equals(df["capacidade_instalada_up"])

    def test_process_where_float_eq(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada = 30"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] == 30]
        )

    def test_process_where_float_gt(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] > 100]
        )

    def test_process_where_float_lt(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada < 100"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] < 100]
        )

    def test_process_where_float_ge(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada >= 100"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] >= 100]
        )

    def test_process_where_float_le(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada <= 100"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] <= 100]
        )

    def test_process_where_datetime_eq(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada = '2023-01-01T00:00:00+00:00'"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                == datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_gt(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada > '2023-01-01T00:00:00+00:00'"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                > datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_ge(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada >= '2023-01-01T00:00:00+00:00'"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                >= datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_lt(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada < '2023-01-01T00:00:00+00:00'"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                < datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_le(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada <= '2023-01-01T00:00:00+00:00'"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                <= datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_in_single_value(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada IN ('2023-01-01T00:00:00+00:00')"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"].isin(
                    [datetime(2023, 1, 1, tzinfo=pytz.utc)]
                )
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_in_ending_comma(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada IN ('2023-01-01T00:00:00+00:00',)"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"].isin(
                    [datetime(2023, 1, 1, tzinfo=pytz.utc)]
                )
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_in_two_values(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada IN ('2023-01-01T00:00:00+00:00', '2023-01-02T00:00:00+00:00')"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"].isin(
                    [
                        datetime(2023, 1, 1, tzinfo=pytz.utc),
                        datetime(2023, 1, 2, tzinfo=pytz.utc),
                    ]
                )
            ].reset_index(drop=True)
        )

    def test_process_where_datetime_not_in_single_value(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada NOT IN ('2023-01-01T00:00:00+00:00')"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                ~expected_df["data_rodada"].isin(
                    [datetime(2023, 1, 1, tzinfo=pytz.utc)]
                )
            ].reset_index(drop=True)
        )

    def test_process_where_date_eq(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao = '2009-08-26'"
        )
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
        )
        expected_df["data_inicio_operacao"] = pd.to_datetime(
            expected_df["data_inicio_operacao"]
        )
        expected_df["data_inicio_simulacao"] = pd.to_datetime(
            expected_df["data_inicio_simulacao"]
        )

        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_inicio_operacao"].isin(
                    [datetime.fromisoformat("2009-08-26")]
                )
            ].reset_index(drop=True)
        )

    def test_process_where_date_gt(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao > '2009-08-26'"
        )
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
        )
        expected_df["data_inicio_operacao"] = pd.to_datetime(
            expected_df["data_inicio_operacao"]
        )
        expected_df["data_inicio_simulacao"] = pd.to_datetime(
            expected_df["data_inicio_simulacao"]
        )

        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_inicio_operacao"]
                > datetime.fromisoformat("2009-08-26")
            ].reset_index(drop=True)
        )

    def test_process_where_date_ge(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao >= '2009-08-26'"
        )
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
        )
        expected_df["data_inicio_operacao"] = pd.to_datetime(
            expected_df["data_inicio_operacao"]
        )
        expected_df["data_inicio_simulacao"] = pd.to_datetime(
            expected_df["data_inicio_simulacao"]
        )

        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_inicio_operacao"]
                >= datetime.fromisoformat("2009-08-26")
            ].reset_index(drop=True)
        )

    def test_process_where_date_lt(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao < '2009-08-26'"
        )
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
        )
        expected_df["data_inicio_operacao"] = pd.to_datetime(
            expected_df["data_inicio_operacao"]
        )
        expected_df["data_inicio_simulacao"] = pd.to_datetime(
            expected_df["data_inicio_simulacao"]
        )

        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_inicio_operacao"]
                < datetime.fromisoformat("2009-08-26")
            ].reset_index(drop=True)
        )

    def test_process_where_date_le(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao <= '2009-08-26'"
        )
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
        )
        expected_df["data_inicio_operacao"] = pd.to_datetime(
            expected_df["data_inicio_operacao"]
        )
        expected_df["data_inicio_simulacao"] = pd.to_datetime(
            expected_df["data_inicio_simulacao"]
        )

        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_inicio_operacao"]
                <= datetime.fromisoformat("2009-08-26")
            ].reset_index(drop=True)
        )

    def test_process_where_partition_alias(self):
        conn = FSConnection("tests/data")
        query = "SELECT nome AS nome_usina, subsistema_geografico AS subsis FROM usinas_part_subsis WHERE subsis = 'NE';"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        assert len(process_result["processedFiles"]) == 1
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas_part_subsis/usinas_part_subsis-subsistema_geografico=NE.parquet.gzip",
        )
        expected_df = expected_df.rename(columns={"nome": "nome_usina"})
        expected_df["subsis"] = "NE"
        assert df.reset_index(drop=True).equals(
            expected_df.reset_index(drop=True)[["nome_usina", "subsis"]]
        )

    def test_process_where_partition_table_alias(self):
        conn = FSConnection("tests/data")
        query = "SELECT u.nome AS nome_usina, u.subsistema_geografico AS subsis FROM usinas_part_subsis AS u WHERE subsis = 'NE';"
        tokens = query2tokens(query)
        process_result = SELECT.process(tokens[1:], conn)
        assert len(process_result["processedFiles"]) == 1
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas_part_subsis/usinas_part_subsis-subsistema_geografico=NE.parquet.gzip",
        )
        expected_df = expected_df.rename(columns={"nome": "nome_usina"})
        expected_df["subsis"] = "NE"
        assert df.reset_index(drop=True).equals(
            expected_df.reset_index(drop=True)[["nome_usina", "subsis"]]
        )
