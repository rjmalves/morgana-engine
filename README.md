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

### Schema files (schema.json)

The morgana uses some database description file formats which are expected to be located in the same directories that the data files are stored. All the description files are expected to be called `schema.json`, but some fields are mandatory for describing databases or tables. An example of schema file for describing a database stored in S3 is:

```json
{
    "application": "morgana-engine-tests",
    "name": "data",
    "description": "Tests for the morgana engine",
    "uri": "s3://my-bucket/data/schema.json",
    "version": "1.0.0",
    "modifiedTime": "2024-01-01T00:00:00.000Z",
    "tables": [
        {
            "name": "quadriculas",
            "uri": "s3://my-bucket/data/quadriculas/"
        },
        {
            "name": "velocidade_vento_100m",
            "uri": "s3://my-bucket/data/velocidade_vento_100m/"
        }
    ]
}
```

The `uri` fields in the `tables` are expected to point to valid paths (or object keys) with the which should contain another `schema.json` files, describing each table. The `name` fields are the names that are given in the queries made to the morgana engine.

As an example, one of the tables might be described by:

```json
{
    "name": "velocidade_vento_100m",
    "description": "",
    "uri": "s3://my-bucket/data/velocidade_vento_100m/schema.json",
    "fileType": ".parquet.gzip",
    "columns": [
        {
            "name": "data_rodada",
            "type": "datetime"
        },
        {
            "name": "data_previsao",
            "type": "datetime"
        },
        {
            "name": "dia_previsao",
            "type": "integer"
        },
        {
            "name": "valor",
            "type": "number"
        }
    ],
    "partitions": [
        {
            "name": "quadricula",
            "type": "integer"
        }
    ]
}
```

The table as a whole is made of both `columnns` data and `partitions` data, but the partition is obtained by parsing the `filename` of each file in the path which has the table `name` as prefix and the extension given by `fileType`. These fields are the key for optimizing the query times in morgana, so they must be chosen well, so that most queries only join few partitions.

The supported file types are:
- `.csv` (currently does not make any type castings when reading)
- `.parquet`
- `.parquet.gzip` (enforces `gzip` compression using `arrow` backend)

The supported data types are:

- `string`
- `integer`
- `number` (called `float` in most programming languages)
- `bool`
- `date`
- `datetime`

Currently, both `date` and `datetime` are handled by the same backend functions (TODO - implement different parsing backends). Only `string` and `integer` data types are supported for implementing partitions, where the `integer` is always the most recommended for performance improvements.


### SQL Language Support

Currently the morgana only supports the SELECT statement from the SQL language, allowing for generic filters with the WHERE clause. The handling of this statement is customized for better reading of highly partitioned tables, reducing the query processing time when the filter is made on one of the partitions.

When comparing datetime or date columns, no casting is made with respect to the format that is given. The date or datetime values for filters are expected to be in ISO 8601 format, with optional timezone information when the dataframe was written to the. For instance, datetime columns consider timezone information, so the desired filters must be given in the full format.

Also, aliases are supported for column names and table names in queries using the `AS` keyword, with the exception being the column name on which a table is partitioned (TODO - support this feature). 

Some query examples, given the same data schemas described above:

- `SELECT * FROM velocidade_vento_100m WHERE quadricula = 0;`
- `SELECT * FROM velocidade_vento_100m WHERE quadricula IN (1, 2, 3);`
- `SELECT v.quadricula, v.data_previsao, v.valor FROM velocidade_vento_100m AS v WHERE v.quadricula > 5 AND v.quadricula < 10;`
- `SELECT quadricula, data_rodada as rodada, dia_previsao AS d, data_previsao AS data FROM velocidade_vento_100m WHERE quadricula = 1000 AND rodada >= '2023-01-01T00:00:00+00:00' AND d = 1;`


## Contributing

TODO