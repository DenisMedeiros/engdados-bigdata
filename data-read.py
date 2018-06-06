import csv, os
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.resultados2014


header = ['DATA_GERACAO',
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
'TOTAL_VOTOS']


for filename in os.listdir('dataset/votacao'):
  arquivo = (open('dataset/votacao/' + filename,'r'))  
  
  reader = csv.DictReader(arquivo,fieldnames=header, delimiter=';')
  for line in reader:    
    del line[None]
    collection.insert_one(line)
    
  arquivo.close()
  
  
  



