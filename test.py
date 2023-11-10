from app.adapters.repository.connection import factory as conn_factory
from app.adapters.repository.dataio import factory as io_factory
from app.adapters.repository.partitioner import factory as part_factory
from datetime import date
from app.services.handlers import parse


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

df = parse(raw, conn)

df
