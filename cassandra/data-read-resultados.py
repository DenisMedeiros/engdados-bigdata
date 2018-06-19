import os, pandas
from cassandra.cluster import Cluster

header = [
'DATA_GERACAO',
'HORA_GERACAO',
'ANO_ELEICAO',
'NUM_TURNO',
'DESCRICAO_ELEICAO',
'SIGLA_UF',
'SIGLA_UE',
'CODIGO_MUNICIPIO',
'NOME_MUNICIPIO',
'NUMERO_ZONA',
'CODIGO_CARGO',
'NUMERO_CAND',
'SQ_CANDIDATO',
'NOME_CANDIDATO',
'NOME_URNA_CANDIDATO',
'DESCRICAO_CARGO',
'COD_SIT_CAND_SUPERIOR',
'DESC_SIT_CAND_SUPERIOR',
'CODIGO_SIT_CANDIDATO',
'DESC_SIT_CANDIDATO',
'CODIGO_SIT_CAND_TOT',
'DESC_SIT_CAND_TOT',
'NUMERO_PARTIDO',
'SIGLA_PARTIDO',
'NOME_PARTIDO',
'SEQUENCIAL_LEGENDA',
'NOME_COLIGACAO',
'COMPOSICAO_LEGENDA',
'TOTAL_VOTOS',
'LIXO'
]

DIRPATH = './dados/resultados/'

cluster = Cluster()
session = cluster.connect('eleicoes')

for filename in os.listdir(DIRPATH):
    df = pandas.read_csv(os.path.join(DIRPATH, filename), sep=';', names=header, encoding='utf-8')
    for index, row in df.iterrows():
        
        session.execute(
            """
            INSERT INTO resultados2014(ID, SIGLA_UF, CODIGO_MUNICIPIO, NOME_MUNICIPIO, SQ_CANDIDATO, CODIGO_SIT_CAND_TOT, DESC_SIT_CAND_TOT, TOTAL_VOTOS) VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s)
            """,
            (row['SIGLA_UF'], row['CODIGO_MUNICIPIO'], row['NOME_MUNICIPIO'], row['SQ_CANDIDATO'], row['CODIGO_SIT_CAND_TOT'], row['DESC_SIT_CAND_TOT'], row['TOTAL_VOTOS'])
        )

session.shutdown()



