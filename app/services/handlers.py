from app.adapters.repository.connection import Connection
from app.adapters.repository.processing import factory
from app.utils.sql import query2tokens
import pandas as pd


def process_query(
    query: str,
    conn: Connection,
) -> pd.DataFrame | dict | None:
    """
    Process a SQL query and return the result.

    Args:
        query (str): The SQL query to be processed.
        conn (Connection): The database connection object.

    Returns:
        Union[pd.DataFrame, dict, None]: The result of the query.
    """
    # TODO - validate query:
    # - 1 statement only
    # - among the supported keywords
    # ...
    try:
        tokens = query2tokens(query)
        processer = factory(tokens[0].normalized)
        r = processer.process(tokens, conn)
        return r
    except Exception as e:
        return {"error": str(e)}
