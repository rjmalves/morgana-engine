from morgana_engine import select_lambda_endpoint


def lambda_handler(event, context):
    res = select_lambda_endpoint(event)
    return res
