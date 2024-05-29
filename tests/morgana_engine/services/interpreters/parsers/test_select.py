from morgana_engine.services.interpreters.lex import lex
from morgana_engine.services.interpreters.parse import parse
from morgana_engine.adapters.repository.connection import FSConnection
import pandas as pd
import pytz
from datetime import datetime


class TestSELECT:
    def test_select(self):
        conn = FSConnection("tests/data")
        columns = [
            "id",
            "codigo",
            "nome",
            "capacidade_instalada",
        ]
        query_cols = ",".join(columns)
        query = f"SELECT {query_cols} FROM usinas"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip", columns=columns
        )
        assert df.equals(expected_df)

    def test_join_tables(self):
        conn = FSConnection("tests/data")

        query = """SELECT id, up.id, codigo, up.codigo, nome, up.nome,
                   capacidade_instalada, up.capacidade_instalada
                   FROM usinas
                   INNER JOIN usinas_part_subsis AS up
                   ON usinas.id = up.id"""
        result = parse(lex(query), conn)
        df = result.data
        assert df["codigo"].equals(df["codigo_up"])
        assert df["nome"].equals(df["nome_up"])
        assert df["capacidade_instalada"].equals(df["capacidade_instalada_up"])

    def test_where_float_eq(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada = 30"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] == 30]
        )

    def test_where_float_gt(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] > 100]
        )

    def test_where_float_lt(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada < 100"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] < 100]
        )

    def test_where_float_ge(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada >= 100"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] >= 100]
        )

    def test_where_float_le(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada <= 100"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] <= 100]
        )

    def test_where_datetime_eq(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada = '2023-01-01T00:00:00+00:00'"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                == datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_where_datetime_gt(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada > '2023-01-01T00:00:00+00:00'"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                > datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_where_datetime_ge(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada >= '2023-01-01T00:00:00+00:00'"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                >= datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_where_datetime_lt(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada < '2023-01-01T00:00:00+00:00'"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                < datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_where_datetime_le(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada <= '2023-01-01T00:00:00+00:00'"
        result = parse(lex(query), conn)
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/velocidade_vento_100m/velocidade_vento_100m-quadricula=1.parquet.gzip",
        )
        assert df.reset_index(drop=True).equals(
            expected_df.loc[
                expected_df["data_rodada"]
                <= datetime(2023, 1, 1, tzinfo=pytz.utc)
            ].reset_index(drop=True)
        )

    def test_where_datetime_in_single_value(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada IN ('2023-01-01T00:00:00+00:00')"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_datetime_in_ending_comma(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada IN ('2023-01-01T00:00:00+00:00',)"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_datetime_in_two_values(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada IN ('2023-01-01T00:00:00+00:00', '2023-01-02T00:00:00+00:00')"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_datetime_not_in_single_value(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM velocidade_vento_100m WHERE data_rodada NOT IN ('2023-01-01T00:00:00+00:00')"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_date_eq(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM usinas WHERE data_inicio_operacao = '2009-08-26'"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_date_gt(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM usinas WHERE data_inicio_operacao > '2009-08-26'"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_date_ge(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao >= '2009-08-26'"
        )
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_date_lt(self):
        conn = FSConnection("tests/data")
        query = "SELECT * FROM usinas WHERE data_inicio_operacao < '2009-08-26'"
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_date_le(self):
        conn = FSConnection("tests/data")
        query = (
            "SELECT * FROM usinas WHERE data_inicio_operacao <= '2009-08-26'"
        )
        result = parse(lex(query), conn)
        df = result.data
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

    def test_where_partition_alias(self):
        conn = FSConnection("tests/data")
        query = "SELECT nome AS nome_usina, subsistema_geografico AS subsis FROM usinas_part_subsis WHERE subsis = 'NE';"
        result = parse(lex(query), conn)
        # assert len(process_result["processedFiles"]) == 1
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas_part_subsis/usinas_part_subsis-subsistema_geografico=NE.parquet.gzip",
        )
        expected_df = expected_df.rename(columns={"nome": "nome_usina"})
        expected_df["subsis"] = "NE"
        assert df.reset_index(drop=True).equals(
            expected_df.reset_index(drop=True)[["nome_usina", "subsis"]]
        )

    def test_where_partition_table_alias(self):
        conn = FSConnection("tests/data")
        query = "SELECT u.nome AS nome_usina, u.subsistema_geografico AS subsis FROM usinas_part_subsis AS u WHERE subsis = 'NE';"
        result = parse(lex(query), conn)
        # assert len(process_result["processedFiles"]) == 1
        df = result.data
        expected_df = pd.read_parquet(
            "tests/data/usinas_part_subsis/usinas_part_subsis-subsistema_geografico=NE.parquet.gzip",
        )
        expected_df = expected_df.rename(columns={"nome": "nome_usina"})
        expected_df["subsis"] = "NE"
        assert df.reset_index(drop=True).equals(
            expected_df.reset_index(drop=True)[["nome_usina", "subsis"]]
        )
