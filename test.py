from app.adapters.repository.connection import factory as conn_factory
from app.adapters.repository.dataio import factory as io_factory
from app.adapters.repository.partitioner import factory as part_factory
from datetime import date
from app.services.handlers import parse


raw = """
SELECT C.NOME, C.SEXO, E.BAIRRO, E.CIDADE, T.TIPO, T.NUMERO
FROM CLIENTE C
INNER JOIN ENDERECO E
ON C.ID = E.ID_CLIENTE
INNER JOIN TELEFONE T
ON C.ID = T.ID_CLIENTE;
"""

raw = """
SELECT codigo, nome, capacidade_instalada
FROM usinas;
"""


conn = conn_factory("FS")("/home/rogerio/git/dbrenovaveispy/tests/data")

parse(raw, conn)

io = io_factory("")
io.read()


part = part_factory("range")
part.find(
    conn.schema,
    {"data_inicio_operacao": [date(2024, 10, 1), None]},
)


import pandas as pd

cols = "codigo, nome, capacidade_instalada"

df = pd.read_parquet(
    "./tests/data/usinas/data.parquet.gzip",
    columns=[c.strip() for c in cols.split(",")],
)

df.query("")
