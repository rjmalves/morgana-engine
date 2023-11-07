import sqlparse
from typing import List
from sqlparse.sql import (
    Token,
    Parenthesis,
    Comparison,
    Where,
    Identifier,
    IdentifierList,
)
from sqlparse.tokens import Newline, Whitespace, Punctuation, DML

from app.utils.sql import identifierlist2dict, aliases2dict, where2pandas


etc_tokens = [Newline, Whitespace, Punctuation]

raw = """
SELECT C.NOME, C.SEXO, E.BAIRRO, E.CIDADE, T.TIPO, T.NUMERO
FROM CLIENTE C
INNER JOIN ENDERECO E
ON C.ID = E.ID_CLIENTE
INNER JOIN TELEFONE T
ON C.ID = T.ID_CLIENTE
WHERE (C.ID > 10) AND (C.ID < 20);
"""

raw = """
SELECT ID, NOME, SEXO
FROM CLIENTE 
WHERE ID > 10 AND ID < 20;
"""


statements = sqlparse.parse(raw)
tokens: List[Token] = [
    t for t in statements[0].tokens if t.ttype not in etc_tokens
]
[type(t) for t in tokens[-1]]
[t.value for t in tokens[-1]]

# identifierlist2dict(tokens[1])
# aliases2dict([t for t in tokens if type(t) == Identifier])
where2pandas(tokens[-1])


t = [t for t in tokens if type(t) == Identifier][0]
t.get_real_name()

[t.value for t in tokens]
[t.ttype for t in tokens]

tokens[-1].tokens
tokens[0].within(Parenthesis)

tokens[-1].within(Where)

t: Token = tokens[3]
t.get_real_name()


t: Identifier = [t for t in tokens if type(t) == Identifier][0]
t.get_real_name()

tlist = [t for t in tokens if type(t) == IdentifierList][0]
tlist = [t for t in tlist.tokens if type(t) == Identifier]
list(set([t.get_parent_name() for t in tlist]))
a = {None: []}

a[None]

t: IdentifierList = tokens[1]
t: Identifier = t.tokens[0]
t.get_parent_name()
t.value
t.get_name()

t.get_real_name()


tokens[0].value

t: IdentifierList = [t for t in tokens if type(t) == IdentifierList][0]


t.tokens

[a for a in t.get_identifiers()]

[a.get for a in t.get_identifiers()]


tokens: List[Token] = [
    t for t in statements[0].tokens if t.ttype not in etc_tokens
]
tokens[3]
tokens[3].get_real_name()

from jsonschema import validate
from jsonschema.validators import Draft202012Validator

validate(
    "2020-10-12",
    {
        "type": "string",
        "format": "date",
        "partitioned": True,
        "partitions": {},
    },
    format_checker=Draft202012Validator.FORMAT_CHECKER,
)

# TODO
# 1. Montar um schema de teste, com um conjunto de dados, sem partição
# 2. Montar um schema de teste, porém com partição.
# 3. Implementar a parte de processamento do schema (acessar o conjunto de
#    dados e compreender o que está armazenado naquela URI).
# 4. Implementar a parte de parsing da query, convertendo uma query SQL em uma
#    adequada para pandas.
# 5. Fazer um esboço de função lambda para ser chamada via AWS.
