#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
from pyspark_cassandra import CassandraSparkContext

SEXO = {
    2: 'MASCULINO', 
    4: 'FEMININO'
}

ESCOLARIDADE = {
    2: 'LÊ E ESCREVE',
    3: 'ENSINO FUNDAMENTAL INCOMPLETO',
    4: 'ENSINO FUNDAMENTAL COMPLETO',
    5: 'ENSINO MÉDIO INCOMPLETO',
    6: 'ENSINO MÉDIO COMPLETO',
    7: 'SUPERIOR INCOMPLETO',
    8: 'SUPERIOR COMPLETO',
}

ETNIA = {
    1: 'BRANCA',
    2: 'PRETA',
    3: 'PARDA',
    4: 'AMARELA',
    5: 'INDÍGENA',
}

# Cria o SparkContext
conf = SparkConf() \
    .setAppName("Pergunta1") \
    .set("spark.cassandra.connection.host", "10.7.40.94")
csc = CassandraSparkContext(conf=conf)

# Prepara os RDDs das tabelas.
candidatos = csc.cassandraTable("eleicoes", "candidatos2014")
resultados = csc.cassandraTable("eleicoes", "resultados2014")

# Busca o código dos candidatos eleitos e dinstintos (para desconsiderar segundo turno).
# Existem 3 tipos de candidatos eleitos (1, 2, 3).

cod_eleitos1 = resultados.select('sq_candidato').where("codigo_sit_cand_tot=? ", 1)
cod_eleitos2 = resultados.select('sq_candidato').where("codigo_sit_cand_tot=? ", 2)
cod_eleitos3 = resultados.select('sq_candidato').where("codigo_sit_cand_tot=? ", 3)

# Une os códigos dos candidatos eleitos (os 3 tipos).
cod_eleitos = cod_eleitos1.union(cod_eleitos2).union(cod_eleitos3).map(lambda row: (row['sq_candidato'], 0))

informacoes = candidatos.select('sequencial_candidato', 'codigo_sexo', 'cod_grau_instrucao', 'codigo_cor_raca', 'despesa_max_campanha').map(lambda row: (row['sequencial_candidato'], (row['codigo_sexo'], row['cod_grau_instrucao'], row['codigo_cor_raca'], row['despesa_max_campanha'])))

# Faz a interseção dos eleitos com as informações deles.
dados = cod_eleitos.join(informacoes)

# Obtém os resultados por sexo.

# Exemplo de entry: (160000000560L, (None, (2, 8, 1, 9250000)))
masculino = dados.filter(lambda entry: entry[1][1][0] == 2)
feminino = dados.filter(lambda entry: entry[1][1][0] == 4)

print 'Homens eleitos: %d' %masculino.count()
print 'Mulheres eleitas: %d' %masculino.count()

saida = masculino.union(masculino)

saida.saveAsTextFile("hdfs://dricardo-master:9000/user/engdados/resultados.rdd", "codec_if_any");



