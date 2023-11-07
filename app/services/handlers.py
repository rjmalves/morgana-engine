from app.adapters.repository.connection import Connection
from app.adapters.repository.dataio import DataIO, ParquetIO
from app.adapters.repository.queryparser import factory as query_factory
from app.utils.sql import query2tokens
import pandas as pd


def parse(
    query: str,
    conn: Connection,
) -> pd.DataFrame | dict | None:
    tokens = query2tokens(query)
    parser = query_factory(tokens[0].normalized)
    r = parser.parse(tokens, conn)
    return r
