from morgana_engine.adapters.repository.processing import SELECT
from morgana_engine.utils.sql import query2tokens, filter_spacing_tokens
from morgana_engine.models.readingfilter import UnequalityReadingFilter
from morgana_engine.adapters.repository.connection import FSConnection
import pandas as pd


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
        filters = SELECT._process_filters_for_reading(tokens[1:])
        assert filters == [(None, UnequalityReadingFilter(filter_tokens))]

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
        df = SELECT._process_select_from_table(table, filters, columns, conn)
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
        dfs = SELECT._process_select(table, filters, columns, conn)
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
        dfs = SELECT._process_select(table, filters, columns, conn)
        df = SELECT._process_join_tables(
            dfs, [("id", "usinas_part_subsis_id")]
        )
        assert df["codigo"].equals(df["usinas_part_subsis_codigo"])
        assert df["nome"].equals(df["usinas_part_subsis_nome"])
        assert df["capacidade_instalada"].equals(
            df["usinas_part_subsis_capacidade_instalada"]
        )

    def test_process(self):
        conn = FSConnection("tests/data")
        query = "SELECT id, codigo, nome, capacidade_instalada FROM usinas WHERE capacidade_instalada > 100"
        tokens = query2tokens(query)
        df = SELECT.process(tokens[1:], conn)
        expected_df = pd.read_parquet(
            "tests/data/usinas/usinas.parquet.gzip",
            columns=["id", "codigo", "nome", "capacidade_instalada"],
        )
        assert df.equals(
            expected_df.loc[expected_df["capacidade_instalada"] > 100]
        )
