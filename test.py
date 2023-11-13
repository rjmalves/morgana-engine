from app.adapters.repository.connection import factory as conn_factory
from datetime import date
from app.services.handlers import process_query


conn = conn_factory("FS")("/home/rogerio/ONS/dados_weol/gfs")

raw = """
SELECT data_rodada, data_previsao, dia_previsao, valor, quadricula
FROM velocidade_vento_100m
WHERE quadricula IN (1,);
"""


def main():
    return process_query(raw, conn)


# raw = """
# SELECT id, lat, lon FROM quadriculas;
# """
# df = process_query(raw, conn)
# print(df)

import tracemalloc

tracemalloc.start()

df = main()
print(df)

print(tracemalloc.get_traced_memory())

tracemalloc.stop()
