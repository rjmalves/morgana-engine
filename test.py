from morgana_engine.services.interpreters.lex import lex
from morgana_engine.services.interpreters.parse import parse
from morgana_engine.adapters.repository.connection import FSConnection

query = """SELECT usinas.id, usinas.codigo, usinas.nome, usinas.capacidade_instalada, usinas.data_inicio_operacao
 FROM usinas INNER JOIN usinas_part_id ON usinas.id = usinas_part_id.id WHERE usinas.capacidade_instalada = 30 AND usinas.data_inicio_operacao NOT IN ('2020-01-01', '2020-01-02')"""

conn = FSConnection("./tests/data")


stmt = lex(query)
print(parse(stmt, conn))
