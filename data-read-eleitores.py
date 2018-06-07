import csv, os
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.eleitores2014


header = ['PERIODO',
'UF',
'MUNICIPIO',
'COD_MUNICIPIO_TSE',
'NR_ZONA',
'SEXO',
'FAIXA_ETARIA',
'GRAU_DE_ESCOLARIDADE',
'QTD_ELEITORES_NO_PERFIL']


for filename in os.listdir('dataset/eleitores'):
  arquivo = (open('dataset/eleitores/' + filename,'r'))

  reader = csv.DictReader(arquivo,fieldnames=header, delimiter=';')
  for line in reader:
    collection.insert_one(line)

  arquivo.close()
