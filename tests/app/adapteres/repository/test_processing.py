from app.adapters.repository.processing import SELECT
from app.utils.sql import query2tokens


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
        print(join_map)
        assert join_map == [("usinas_codigo", "usinas_part_subsis_codigo")]

    # def test_process_filters_for_reading(self):
    #     tokens = [
    #         "SELECT",
    #         "usinas.id",
    #         "usinas.codigo",
    #         "usinas.nome",
    #         "usinas.capacidade_instalada",
    #         "FROM",
    #         "usinas",
    #         "WHERE",
    #         "usinas.capacidade_instalada > 100",
    #     ]
    #     filters = SELECT._SELECT__process_filters_for_reading(tokens)
    #     assert filters == [("usinas", "capacidade_instalada", ">", 100)]

    # def test_process_filters_for_querying(self):
    #     tokens = [
    #         "SELECT",
    #         "usinas.id",
    #         "usinas.codigo",
    #         "usinas.nome",
    #         "usinas.capacidade_instalada",
    #         "FROM",
    #         "usinas",
    #         "WHERE",
    #         "usinas.capacidade_instalada > 100",
    #     ]
    #     tables_to_select = {"usinas": "usinas"}
    #     filters = SELECT._SELECT__process_filters_for_querying(
    #         tokens, tables_to_select
    #     )
    #     assert filters == "usinas.capacidade_instalada > 100"

    # def test_process_select_from_table(self):
    #     conn = FSConnection("tests/data")
    #     table = "usinas"
    #     filters = [("usinas", "capacidade_instalada", ">", 100)]
    #     columns = {
    #         "id": "id",
    #         "codigo": "codigo",
    #         "nome": "nome",
    #         "capacidade_instalada": "capacidade_instalada",
    #     }
    #     df = SELECT._SELECT__process_select_from_table(
    #         table, filters, columns, conn
    #     )
    #     expected_df = pd.DataFrame(
    #         {
    #             "id": [1, 2, 3],
    #             "codigo": ["UHE1", "UHE2", "UHE3"],
    #             "nome": [
    #                 "Usina Hidrelétrica 1",
    #                 "Usina Hidrelétrica 2",
    #                 "Usina Hidrelétrica 3",
    #             ],
    #             "capacidade_instalada": [120.0, 150.0, 200.0],
    #         }
    #     )
    #     assert_frame_equal(df, expected_df)
