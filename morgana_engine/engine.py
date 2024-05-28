from .adapters import connection_factory  # noqa
from .services.interpreters.lex import lex  # noqa
from .services.interpreters.parse import parse  # noqa
from io import BytesIO
import pandas as pd
import base64


def select_lambda_endpoint(
    request_body: dict,
) -> dict:
    conn = connection_factory("S3")(request_body["database"])
    stmt = lex(request_body["query"])
    result = parse(stmt, conn)

    if result.status:
        df = result.data
        assert isinstance(df, pd.DataFrame)
        f = BytesIO()
        df.reset_index(drop=True).to_parquet(f, compression="gzip")
        f.seek(0)
        return {
            "statusCode": 200,
            "body": base64.b64encode(f.read()).decode("utf-8"),
        }
    else:
        return result.__dict__
