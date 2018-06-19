import csv, os
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.resultados2014


header = ['APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'SIGLA_UF',
'APAGAR',
'CODIGO_MUNICIPIO',
'NOME_MUNICIPIO',
'APAGAR',
'APAGAR',
'APAGAR',
'SQ_CANDIDATO',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'CODIGO_SIT_CAND_TOT',
'DESC_SIT_CAND_TOT',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'APAGAR',
'TOTAL_VOTOS']


for filename in os.listdir('../dataset/votacao'):
  arquivo = (open('../dataset/votacao/' + filename,'r'))

  reader = csv.DictReader(arquivo,fieldnames=header, delimiter=';')
  for line in reader:
    del line[None]
    del line['APAGAR']
    line['TOTAL_VOTOS'] = int(line['TOTAL_VOTOS'])
    collection.insert_one(line)

  arquivo.close()
