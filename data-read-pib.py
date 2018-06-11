import csv, os,json
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.pib2014

header = ['valor','municipio','ano']
arquivo = open('dataset/pib/pib.json','rb')
reader = csv.DictReader(arquivo,fieldnames=header,delimiter=',')
for line in reader:
    if None in line:
        del line[None]
    collection.insert_one(line)
arquivo.close()
