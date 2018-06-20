import os, pandas, json
from cassandra.cluster import Cluster

header = [
'VALOR',
'MUNICIPIO',
'ANO'
]

cluster = Cluster(['10.7.40.94', '10.7.40.117'])
session = cluster.connect('eleicoes')

FILEPATH = '../../dados/pibs/pib-cidades.json.pronto'

with open(FILEPATH, 'r') as f:
    data = json.load(f)

df = pandas.DataFrame(data['valores'])

for index, row in df.iterrows():

    session.execute(
        """
        INSERT INTO pibs (ID, VALOR, MUNICIPIO, ANO)
        VALUES (uuid(), %s, %s, %s)
        """,
        (int(row['valor']), int(row['municipio_ibge']), int(row['ano']))
    )

session.shutdown()

