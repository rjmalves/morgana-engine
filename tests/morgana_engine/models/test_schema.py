from morgana_engine.models.schema import Schema


class TestSchema:
    def test_uri(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "tables": [],
        }
        schema = Schema(schema_dict)
        assert schema.uri == "test_uri"

    def test_name(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "tables": [],
        }
        schema = Schema(schema_dict)
        assert schema.name == "test_name"

    def test_format(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "fileType": ".parquet",
            "columns": [],
        }
        schema = Schema(schema_dict)
        assert schema.file_type == ".parquet"

    def test_is_database(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "tables": [],
        }
        schema = Schema(schema_dict)
        assert schema.is_database is True

    def test_is_table(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "fileType": ".parquet",
            "columns": [],
        }
        schema = Schema(schema_dict)
        assert schema.is_table is True

    def test_tables(self):
        schema_dict = {
            "name": "test_name",
            "tables": [
                {"name": "table1", "uri": "ref1"},
                {"name": "table2", "uri": "ref2"},
            ],
        }
        schema = Schema(schema_dict)
        assert schema.tables == {"table1": "ref1", "table2": "ref2"}

    def test_columns(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "columns": [
                {"name": "col1", "type": "int"},
                {"name": "col2", "type": "string"},
            ],
        }
        schema = Schema(schema_dict)
        assert schema.columns == {"col1": "int", "col2": "string"}

    def test_partitions(self):
        schema_dict = {
            "uri": "test_uri",
            "name": "test_name",
            "columns": [],
            "partitions": [
                {"name": "key1", "type": "int"},
                {"name": "key2", "type": "string"},
            ],
        }
        schema = Schema(schema_dict)
        assert schema.partitions == {"key1": "int", "key2": "string"}

    def test_eq(self):
        schema_dict1 = {
            "uri": "test_uri",
            "name": "test_name",
        }
        schema_dict2 = {
            "uri": "test_uri",
            "name": "test_name",
        }
        schema1 = Schema(schema_dict1)
        schema2 = Schema(schema_dict2)
        assert schema1 == schema2
