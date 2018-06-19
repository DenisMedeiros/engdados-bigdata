import os, pandas
from cassandra.cluster import Cluster

header = [
'PERIODO',
'UF',
'MUNICIPIO',
'COD_MUNICIPIO_TSE',
'NR_ZONA',
'SEXO',
'FAIXA_ETARIA',
'GRAU_DE_ESCOLARIDADE',
'QTD_ELEITORES_NO_PERFIL'
]

FILEPATH = '../../dados/eleitores/perfil_eleitorado_ATUAL.txt.pronto'

cluster = Cluster()
session = cluster.connect('eleicoes')

df = pandas.read_csv(FILEPATH, sep=';', names=header, encoding='utf-8')
for index, row in df.iterrows():
    session.execute(
        """
        INSERT INTO eleitores(ID, UF, MUNICIPIO, COD_MUNICIPIO_TSE, FAIXA_ETARIA, GRAU_DE_ESCOLARIDADE, QTD_ELEITORES_NO_PERFIL) VALUES (uuid(), %s, %s, %s, %s, %s, %s)
        """,
        (row['UF'], row['MUNICIPIO'], row['COD_MUNICIPIO_TSE'], row['FAIXA_ETARIA'], row['GRAU_DE_ESCOLARIDADE'], row['QTD_ELEITORES_NO_PERFIL'])
    )

session.shutdown()



