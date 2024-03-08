from morgana_engine.adapters.repository.connection import Connection
from morgana_engine.adapters.repository.processing import factory
from morgana_engine.utils.sql import query2tokens
import traceback


def process_query(
    query: str,
    conn: Connection,
) -> dict:
    """
    Process a SQL query and return the result.

    Args:
        query (str): The SQL query to be processed.
        conn (Connection): The database connection object.

    Returns:
        dict: The result of the query.
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
        traceback.print_exc()
        return {"statusCode": 500, "error": str(e)}
