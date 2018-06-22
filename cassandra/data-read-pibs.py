import os, pandas
from cassandra.cluster import Cluster

header = [
'UF',
'CIDADE',
'PIB',
'POPULACAO',
'PIB_PERCAPITA',
'COD_TSE'
]

cluster = Cluster(['10.7.40.94', '10.7.40.117'])
session = cluster.connect('eleicoes')

FILEPATH = '../pibs-final.csv'

df = pandas.read_csv(FILEPATH, sep=',', names=header, encoding='utf-8')
for index, row in df.iterrows():
    if row['COD_TSE'] != '-':
        session.execute(
        """
        INSERT INTO pibs(ID, UF, CIDADE, PIB, POPULACAO, PIB_PERCAPITA, COD_TSE) VALUES (uuid(), %s, %s, %s, %s, %s, %s)
        """,
        (row['UF'], row['CIDADE'], float(row['PIB']), int(row['POPULACAO']), float(row['PIB_PERCAPITA']), int(row['COD_TSE']))
        )

session.shutdown()

