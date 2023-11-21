# morgana-engine

[![tests](https://github.com/rjmalves/morgana-engine/actions/workflows/tests.yaml/badge.svg?branch=main)](https://github.com/rjmalves/morgana-engine/actions/workflows/tests.yaml)

morgana is a wrapper for parsing and processing SQL queries for extracting data stored in a FileSystem or FileStorage service, enabling partition-optimized reading and filtering for client applications.

## Quick Start

Currently, morgana is designed to be executed in serverless computing services, such as Amazon Lambda. However, there is no constraint in the deploying environment, being possible to run in the local FileSystem as a direct import to a python application, or being wrapped in a general-purpose CLI. Other serverless environments may be added in the future.

For using in Amazon Lambda, the data that will be fetched must be in Amazon S3, and the files must be partitioned according to the expected Morgana Schema. The body of the Lambda function can be:


```python
from morgana_engine import select_lambda_endpoint

def lambda_handler(event, context):
    res = select_lambda_endpoint(event)
    return res
```

The function must be invoked by passing a JSON payload with `database` and `query` fields. For instance, using AWS CLI:

```
aws lambda invoke \
    --function-name morgana-engine-demo \
    --cli-binary-format raw-in-base64-out \
    --payload '{ "database": "s3://my-database-bucket", "query": "SELECT * FROM my_table" }' \
    response.json
```

The output of the query, if it succeeds, is a JSON object with the resulting DataFrame in the `body` field, encoded with `base64` and in `parquet` format with `gzip` compression. In order to read the contents of the file, in Python, one might do:

```python

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

```

The contents of the DataFrame:

```
                     data_rodada  ... valor
0      2017-01-01 00:00:00+00:00  ...  6.09
1      2017-01-01 00:00:00+00:00  ...  5.63
2      2017-01-01 00:00:00+00:00  ...  5.12
3      2017-01-01 00:00:00+00:00  ...  4.66
4      2017-01-01 00:00:00+00:00  ...  4.09
...                          ...  ...   ...
365706 2023-06-29 00:00:00+00:00  ...  0.45
365707 2023-06-29 00:00:00+00:00  ...  2.32
365708 2023-06-29 00:00:00+00:00  ...  4.61
365709 2023-06-29 00:00:00+00:00  ...  5.95
365710 2023-06-29 00:00:00+00:00  ...  5.28

[365711 rows x 5 columns]
```

morgana is designed to have a small footprint, allowing the deployment with a reduced amount of RAM and CPU power. The above DataFrame required 55 MB for the runtime, and the result was obtained within few seconds.

## Documentation

TODO

## Contributing

TODO