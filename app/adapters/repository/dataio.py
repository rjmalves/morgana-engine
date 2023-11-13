from abc import ABC
import pandas as pd


class DataIO(ABC):
    """
    Abstract base class for reading and writing data files as pandas DataFrames.
    """

    EXTENSION = ".data"

    @classmethod
    def filter_data_files(cls, files: list[str], *args, **kwargs) -> list[str]:
        """
        Filters the files that are associated with the current format among the
        existing files in the database.
        """
        return [f.split(cls.EXTENSION)[0] for f in files if cls.EXTENSION in f]

    @classmethod
    def read(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        """
        Reads a file identified by a given path as a DataFrame.
        """
        raise NotImplementedError

    @classmethod
    def write(cls, df: pd.DataFrame, path: str, *args, **kwargs):
        """
        Writes a DataFrame in a given path.
        """
        raise NotImplementedError


class ParquetIO(DataIO):
    EXTENSION = ".parquet.gzip"

    @classmethod
    def read(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(path + cls.EXTENSION, *args, **kwargs)

    @classmethod
    def write(cls, df: pd.DataFrame, path: str, *args, **kwargs):
        if "compression" not in kwargs:
            kwargs["compression"] = "gzip"
        df.to_parquet(path, *args, **kwargs)


class CSVIO(DataIO):
    EXTENSION = ".csv"

    @classmethod
    def read(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        return pd.read_csv(path + cls.EXTENSION, *args, **kwargs)

    @classmethod
    def write(cls, df: pd.DataFrame, path: str, *args, **kwargs):
        if "index" not in kwargs:
            kwargs["index"] = False
        df.to_csv(path, *args, **kwargs)


MAPPING: dict[str, type[DataIO]] = {
    "PARQUET": ParquetIO,
    "CSV": CSVIO,
}


def factory(kind: str) -> type[DataIO]:
    return MAPPING.get(kind, ParquetIO)
