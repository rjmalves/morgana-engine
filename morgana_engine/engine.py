from .adapters import connection_factory  # noqa
from .services import process_query  # noqa
from io import BytesIO
import base64


def select_lambda_endpoint(
    request_body: dict,
) -> dict:
    conn = connection_factory("S3")(request_body["database"])
    result = process_query(request_body["query"], conn)

    if result["statusCode"] == 200:
        df = result["data"]
        f = BytesIO()
        df.reset_index(drop=True).to_parquet(f, compression="gzip")
        f.seek(0)
        return {
            "statusCode": 200,
            "body": base64.b64encode(f.read()).decode("utf-8"),
        }
    else:
        return result
