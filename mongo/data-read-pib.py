import csv, os,json
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.pibs

header = ['VALOR','MUNICIPIO','ANO']
arquivo = open('../dataset/pib/pib.json','rb')
reader = csv.DictReader(arquivo,fieldnames=header,delimiter=',')
for line in reader:
    if None in line:
        del line[None]
    line['VALOR'] = int(line['VALOR'])
    collection.insert_one(line)
arquivo.close()
