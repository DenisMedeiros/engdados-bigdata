import csv, os,json
from pymongo import MongoClient

#mongo setup
client = MongoClient('10.7.40.54',27017)
db = client.eleicoes
collection = db.pibs

header = [
'UF',
'CIDADE',
'PIB',
'POPULACAO',
'PIB_PERCAPITA',
'COD_TSE'
]
arquivo = open('../dataset/pib/pibs-final.csv','rb')
reader = csv.DictReader(arquivo,fieldnames=header,delimiter=',')
for line in reader:
    if None in line:
        del line[None]
    line['PIB'] = float(line['PIB'])
    line['PIB_PERCAPITA'] = float(line['PIB_PERCAPITA'])
    line['POPULACAO'] = int(line['POPULACAO'])
    collection.insert_one(line)
arquivo.close()
