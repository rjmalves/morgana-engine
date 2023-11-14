import pytest
import pandas as pd
from morgana_engine.adapters.repository.dataio import DataIO, ParquetIO


class TestDataIO:
    def test_filter_data_files(self):
        files = ["file1.data", "file2.data", "file3.data", "file4.data"]
        filtered_files = DataIO.filter_data_files(files)
        assert filtered_files == ["file1", "file2", "file3", "file4"]

    def test_read_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataIO.read("path/to/file")

    def test_write_not_implemented(self):
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        with pytest.raises(NotImplementedError):
            DataIO.write(df, "path/to/file")


class TestParquetIO:
    def test_read(self, tmp_path):
        # create a test parquet file
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        path = str(tmp_path / "test")
        df.to_parquet(path + ParquetIO.EXTENSION, compression="gzip")

        # read the file using ParquetIO
        result = ParquetIO.read(path)

        # check if the dataframes are equal
        pd.testing.assert_frame_equal(result, df)

    def test_write(self, tmp_path):
        # create a test dataframe
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        # write the dataframe to a parquet file using ParquetIO
        path = str(tmp_path / "test")
        ParquetIO.write(df, path + ParquetIO.EXTENSION)

        # read the file using pandas
        result = pd.read_parquet(path + ParquetIO.EXTENSION)

        # check if the dataframes are equal
        pd.testing.assert_frame_equal(result, df)
