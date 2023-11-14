from app.app import select_lambda_endpoint
from time import time
import tracemalloc

tracemalloc.start()

ti = time()


request_body = {
    "database": "s3://ons-pem-historico/eolica/backup/nwp/gfs/",
    "query": """
        SELECT data_rodada, data_previsao, dia_previsao, quadricula, valor
        FROM velocidade_vento_100m
        WHERE quadricula IN (1, );
        """,
}
df = select_lambda_endpoint(request_body["database"], request_body["query"])
print(df)

tf = time()

print("\nTraced Memory (Current, Peak): ", tracemalloc.get_traced_memory())

tracemalloc.stop()

print(f"Tempo de execução: {tf - ti:.2f} s")
