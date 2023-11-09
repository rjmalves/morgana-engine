from abc import ABC
from app.utils.interval import Interval


class Partitioner(ABC):
    @classmethod
    def columns(cls, schema: dict) -> list[str]:
        raise NotImplementedError

    @classmethod
    def find(cls, schema: dict, filters: dict) -> list[str]:
        raise NotImplementedError


class NotPartitioner(Partitioner):
    @classmethod
    def columns(cls, schema: dict) -> list[str]:
        p_cols: list[str] = []
        for col, props in schema["properties"].items():
            if "partitions" not in props:
                p_cols.append(col)
        return p_cols

    @classmethod
    def find(cls, schema: dict, filters: dict) -> list[str]:
        return [schema["data"]]


class ListPartitioner(Partitioner):
    @classmethod
    def columns(cls, schema: dict) -> list[str]:
        p_cols: list[str] = []
        for col, props in schema["properties"].items():
            if "partitions" in props:
                if props["partitions"]["type"] == "list":
                    p_cols.append(col)
        return p_cols

    @classmethod
    def find(cls, schema: dict, filters: dict) -> list[str]:
        files: list[str] = []
        for col, values in filters.items():
            files_col: dict = schema["properties"][col]["partitions"][
                "mappings"
            ]
            for file, file_col_values in files_col.items():
                if any([v in file_col_values for v in values]):
                    files.append(file)
        return list(set(files))


class RangePartitioner(Partitioner):
    @classmethod
    def columns(cls, schema: dict) -> list[str]:
        p_cols: list[str] = []
        for col, props in schema["properties"].items():
            if "partitions" in props:
                if props["partitions"]["type"] == "range":
                    p_cols.append(col)
        return p_cols

    @classmethod
    def find(cls, schema: dict, filters: dict) -> list[str]:
        files: list[str] = []
        for col, limits in filters.items():
            interval = Interval(*limits)
            files_col: dict = schema["properties"][col]["partitions"][
                "mappings"
            ]
            for file, file_col_limits in files_col.items():
                col_interval = Interval(*file_col_limits)
                if interval.intersects(col_interval):
                    files.append(file)
        return list(set(files))


MAPPING: dict[str, type[Partitioner]] = {
    "list": ListPartitioner,
    "range": RangePartitioner,
}


def factory(kind: str) -> type[Partitioner]:
    return MAPPING.get(kind, NotPartitioner)
