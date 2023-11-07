from datetime import date, datetime
from typing import Callable
import pandas as pd

def __cast_if_valid(value: str | None, format: str):
    __casting_functions: dict[str, Callable] = {
        "date": lambda value: date.fromisoformat(value),
        "datetime": lambda value: datetime.fromisoformat(value),
    }

    return __casting_functions[format](value) if value else None


def enforce_column_types(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    __casting_functions: dict[str, Callable] = {
        "date": pd.to_datetime,
        "datetime": pd.to_datetime,
    }
    df = df.copy()
    for prop, attrs in schema["properties"].items():
        if "type" not in attrs:
            continue
        if (attrs["type"] == "string") and ("format" in attrs):
            if prop in df.columns:
                df[prop] = __casting_functions[attrs["format"]](df[prop])

    return df

def enforce_property_types(schema: dict):
    for prop, attrs in schema["properties"].items():
        if "type" not in attrs:
            continue
        if (attrs["type"] == "string") and ("format" in attrs):
            if "partitions" in attrs:
                partition_data = schema["properties"][prop]["partitions"]
                if "mappings" in partition_data:
                    partition_mappings: dict = partition_data["mappings"]
                    for filename, values in partition_mappings.items():
                        partition_mappings[filename] = [
                            __cast_if_valid(v, attrs["format"]) for v in values
                        ]
