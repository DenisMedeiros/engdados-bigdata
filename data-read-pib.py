import csv, os
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.pib2014


for filename in os.listdir('dataset/pib'):
  arquivo = (open('dataset/pib/' + filename,'r'))

  reader = csv.DictReader(arquivo, delimiter=';')
  for line in reader:
    collection.insert_many(line)

  arquivo.close()
