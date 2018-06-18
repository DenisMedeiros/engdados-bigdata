import csv, os
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('eleicoes')

header = ['VALOR','MUNICIPIO','ANO']
arquivo = open('../pib.csv','r')
reader = csv.DictReader(arquivo,fieldnames=header,delimiter=',')
for line in reader:
    if None in line:
        del line[None]
 
    ''''
    session.execute(
        """
        INSERT INTO pibs (ID, VALOR, MUNICIPIO, ANO)
        VALUES (uuid(), %s, %s, %s)
        """,
        (int(line['VALOR']), int(line['MUNICIPIO']), int(line['ANO']))
    )
    '''

arquivo.close()
session.shutdown()
