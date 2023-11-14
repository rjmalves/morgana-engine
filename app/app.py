from .adapters import connection_factory  # noqa
from .services import process_query  # noqa
import pandas as pd


def select_lambda_endpoint(
    database_bucket_path: str,
    query: str,
) -> pd.DataFrame | dict | None:
    conn = connection_factory("S3")(database_bucket_path)
    return process_query(query, conn)
