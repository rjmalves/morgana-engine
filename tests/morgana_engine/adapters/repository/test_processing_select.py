from morgana_engine.adapters.repository.processing import SELECT
from morgana_engine.utils.sql import query2tokens, filter_spacing_tokens
from morgana_engine.models.readingfilter import UnequalityReadingFilter
from morgana_engine.adapters.repository.connection import FSConnection
import pandas as pd
import pytz
from datetime import datetime, date


class TestSELECT:
    def test_process_tables_identifiers(self):
        query = "SELECT usinas.id, usinas.codigo, usinas.nome, usinas.capacidade_instalada FROM usinas"
        tokens = query2tokens(query)
        alias_map = SELECT._process_table_identifiers(tokens)
        assert alias_map == {None: "usinas"}

    def test_process_column_identifiers(self):
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas"
        tokens = query2tokens(query)
        tables_to_select = {None: "usinas"}
        column_map = SELECT._process_column_identifiers(
            tokens, tables_to_select
        )
        assert column_map == {
            None: {
                "id": "id",
                "codigo": "codigo",
                "nome": "nome",
                "capacidade_instalada": "capacidade_instalada",
            }
        }

    def test_process_join_mappings(self):
        query = "SELECT u.id, u.codigo, up.codigo FROM usinas u INNER JOIN usinas_part_subsis up ON u.codigo = up.codigo"

        tokens = query2tokens(query)
        tables_to_select = {
            "u": "usinas",
            "up": "usinas_part_subsis",
        }
        join_map = SELECT._process_join_mappings(tokens, tables_to_select)
        assert join_map == [("usinas_codigo", "usinas_part_subsis_codigo")]

    def test_process_filters_for_reading(self):
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        tokens = query2tokens(query)
        filter_tokens = filter_spacing_tokens(
            filter_spacing_tokens(tokens[-1].tokens)[-1].tokens
        )
        tables_to_select = SELECT._process_table_identifiers(tokens)
        columns_in_each_table = SELECT._process_column_identifiers(
            tokens, tables_to_select
        )
        filters = SELECT._process_filters_for_reading(
            tokens[1:], columns_in_each_table
        )
        assert filters == [
            (
                None,
                UnequalityReadingFilter(
                    filter_tokens,
                    {v: k for k, v in columns_in_each_table[None].items()},
                ),
            )
        ]

    def test_process_filters_for_querying(self):
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        tokens = query2tokens(query)
        tables_to_select = {None: "usinas"}
        filters = SELECT._process_filters_for_querying(
            tokens, tables_to_select
        )
        assert filters == "capacidade_instalada > 100"

    def test_process_select_from_table(self):
        conn = FSConnection("tests/data")
        table = "usinas"
        filters = []
        columns = {
            "id": "id",
            "codigo": "codigo",
            "nome": "nome",
            "capacidade_instalada": "capacidade_instalada",
        }
        process_result = SELECT._process_select_from_table(
            table, filters, columns, conn
        )
        df = process_result["data"]
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=list(columns.keys()),
        )
        assert df.equals(expected_df)

    def test_process_select(self):
        conn = FSConnection("tests/data")
        table = {None: "usinas"}
        filters = []
        columns = {
            None: {
                "id": "id",
                "codigo": "codigo",
                "nome": "nome",
                "capacidade_instalada": "capacidade_instalada",
            }
        }
        process_result = SELECT._process_select_from_tables(
            table, filters, columns, conn
        )
        dfs = process_result["data"]
        assert len(dfs) == 1
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=list(columns[None].keys()),
        )
        assert dfs[0].equals(expected_df)

    def test_process_join_tables(self):
        conn = FSConnection("tests/data")
        table = {None: "usinas", "up": "usinas_part_subsis"}
        columns = {
            None: {
                "id": "id",
                "codigo": "codigo",
                "nome": "nome",
                "capacidade_instalada": "capacidade_instalada",
            },
            "up": {
                "id": "usinas_part_subsis_id",
                "codigo": "usinas_part_subsis_codigo",
                "nome": "usinas_part_subsis_nome",
                "capacidade_instalada": "usinas_part_subsis_capacidade_instalada",
            },
        }
        filters = []
        process_result = SELECT._process_select_from_tables(
            table, filters, columns, conn
        )
        dfs = process_result["data"]
        df = SELECT._process_join_tables(
            dfs, [("id", "usinas_part_subsis_id")]
        )
        assert df["codigo"].equals(df["usinas_part_subsis_codigo"])
        assert df["nome"].equals(df["usinas_part_subsis_nome"])
        assert df["capacidade_instalada"].equals(
            df["usinas_part_subsis_capacidade_instalada"]
        )

    def test_process_where_integer_eq(self):
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

    def test_process_where_integer_gt(self):
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

    def test_process_where_integer_lt(self):
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

    def test_process_where_integer_ge(self):
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

    def test_process_where_integer_le(self):
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
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1-.parquet.gzip",
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
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1-.parquet.gzip",
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
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1-.parquet.gzip",
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
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1-.parquet.gzip",
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
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1-.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                <= datetime(2023, 1, 1, tzinfo=pytz.utc)
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
                expected_df["data_inicio_operacao"]
                == datetime.fromisoformat("2009-08-26")
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
            "tests/data/usinas_part_subsis/usinas_part_subsis-subsistema_geografico=NE-.parquet.gzip",
        )
        expected_df = expected_df.rename(columns={"nome": "nome_usina"})
        expected_df["subsis"] = "NE"
        assert df.reset_index(drop=True).equals(
            expected_df.reset_index(drop=True)[["nome_usina", "subsis"]]
        )
