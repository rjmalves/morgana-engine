from abc import ABC
from os import listdir
import pandas as pd


class DataIO(ABC):
    pass

    @classmethod
    def read(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        pass

    @classmethod
    def filter_data_files(cls, files: list[str], *args, **kwargs) -> list[str]:
        pass

    @classmethod
    def write(path: str, *args, **kwargs) -> pd.DataFrame:
        pass


class ParquetIO(DataIO):
    EXTENSION = ".parquet.gzip"

    @classmethod
    def read(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        filename = path if cls.EXTENSION in path else path + cls.EXTENSION
        return pd.read_parquet(filename, *args, **kwargs)

    @classmethod
    def filter_data_files(cls, files: list[str], *args, **kwargs) -> list[str]:
        return [f.split(cls.EXTENSION)[0] for f in files if cls.EXTENSION in f]

    @classmethod
    def write(cls, path: str, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError


class CSVIO(DataIO):
    pass


MAPPING: dict[str, type[DataIO]] = {
    "PARQUET": ParquetIO,
    "CSV": CSVIO,
}


def factory(kind: str) -> type[DataIO]:
    return MAPPING.get(kind, ParquetIO)
