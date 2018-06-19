import csv, os
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.candidatos2014


header = ['APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',,
'APAGAR',
'SIGLA_UF',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'SEQUENCIAL_CANDIDATO',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'SIGLA_PARTIDO',
'NOME_PARTIDO',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'CODIGO_SEXO',
'DESCRICAO_SEXO',
'COD_GRAU_INSTRUCAO',
'DESCRICAO_GRAU_INSTRUCAO',
'APAGAR',
'APAGAR',
'CODIGO_COR_RACA',
'DESCRICAO_COR_RACA',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'DESPESA_MAX_CAMPANHA',
'APAGAR',
'APAGAR',
'APAGAR']


for filename in os.listdir('../dataset/candidatos'):
  arquivo = (open('../dataset/candidatos/' + filename,'r'))

  reader = csv.DictReader(arquivo,fieldnames=header, delimiter=';')
  for line in reader:
    del line['APAGAR']
    line['DESPESA_MAX_CAMPANHA'] = int(line['DESPESA_MAX_CAMPANHA'])
    collection.insert_one(line)

  arquivo.close()
