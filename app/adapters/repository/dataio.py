from abc import ABC
import pandas as pd


class DataIO(ABC):
    pass

    @classmethod
    def read(cls, path: str, options: dict) -> pd.DataFrame:
        pass

    @classmethod
    def write(path: str, options: dict) -> pd.DataFrame:
        pass


class ParquetIO(DataIO):
    pass

    @classmethod
    def read(cls, path: str, options: dict) -> pd.DataFrame:
        return pd.read_parquet(path, storage_options=options)

    @classmethod
    def write(cls, path: str, options: dict) -> pd.DataFrame:
        raise NotImplementedError


class CSVIO(DataIO):
    pass


MAPPING: dict[str, type[DataIO]] = {
    "PARQUET": ParquetIO,
    "CSV": CSVIO,
}


def factory(kind: str) -> type[DataIO]:
    return MAPPING.get(kind, ParquetIO)
