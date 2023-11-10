from app.adapters.repository.connection import factory as conn_factory
from datetime import date
from app.services.handlers import process_query


conn = conn_factory("FS")("/home/rogerio/git/dbrenovaveispy/tests/data")

raw = """
SELECT id, codigo AS cod, nome
FROM usinas_part_id
WHERE id in (1,);
"""

# raw = """
# SELECT v.id_usina, v.id_modelo, v.datahora, v.valor AS verificado, p.valor AS previsto
# FROM verificados v
# INNER JOIN previstos p
# ON v.datahora = p.datahora
# WHERE id = 1;
# """

df = process_query(raw, conn)

df
