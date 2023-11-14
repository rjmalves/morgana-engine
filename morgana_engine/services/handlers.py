from morgana_engine.adapters.repository.connection import Connection
from morgana_engine.adapters.repository.processing import factory
from morgana_engine.utils.sql import query2tokens
import pandas as pd


def process_query(
    query: str,
    conn: Connection,
) -> pd.DataFrame | dict:
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
        return {"statusCode": 500, "error": str(e)}
