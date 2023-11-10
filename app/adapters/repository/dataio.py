from abc import ABC
import pandas as pd


class DataIO(ABC):
    """
    Abstract base class for reading and writing data files as pandas DataFrames.
    """

    EXTENSION = ""

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
        # TODO - how to properly overload this method?
        df.to_parquet(path, *args, compression="gzip", **kwargs)


class CSVIO(DataIO):
    EXTENSION = ".csv"

    @classmethod
    def read(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        return pd.read_csv(path + cls.EXTENSION, *args, **kwargs)

    @classmethod
    def write(cls, df: pd.DataFrame, path: str, *args, **kwargs):
        # TODO - how to properly overload this method?
        df.to_csv(path, *args, index=False, **kwargs)


MAPPING: dict[str, type[DataIO]] = {
    "PARQUET": ParquetIO,
    "CSV": CSVIO,
}


def factory(kind: str) -> type[DataIO]:
    return MAPPING.get(kind, ParquetIO)
