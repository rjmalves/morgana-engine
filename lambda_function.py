from morgana_engine import select_lambda_endpoint


def lambda_handler(event, context):
    res = select_lambda_endpoint(event)
    return res


request_body = {
    "database": "s3://my-bucket",
    "query": """
        SELECT *
        FROM my_table
        WHERE partition_column IN (1, );
        """,
}
df = lambda_handler(request_body, {})

import json

with open("response.json", "w") as arq:
    json.dump(df, arq)


import json
import base64
from io import BytesIO
import pandas as pd

with open("response.json", "r") as fp:
    json_dict = json.load(fp)

buffer = BytesIO()
buffer.write(base64.b64decode(json_dict["body"]))
buffer.seek(0)
df = pd.read_parquet(buffer)
print(df)
