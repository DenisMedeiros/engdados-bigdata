#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

# Cria a sessão.
spark = SparkSession.builder.appName("Pergunta2").getOrCreate()

# Gera um DataFrame com os resultados da eleição de 2014.
eleicoesDF = spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="eleicoes", table="resultados2014").\
    load()

# Obtém o código dos candidatos eleitos e suas cidades.
codigos = eleicoesDF.\
    filter(eleicoesDF['codigo_sit_cand_tot'] < 4).\
    select('sq_candidato', 'codigo_municipio').\
    distinct()

# Gera um DataFrame com os dados dos candidatos.
candidatosDF = spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="eleicoes", table="candidatos2014").\
    load()

# Obtém apenas as informações úteis.
informacoes = candidatosDF.select('sequencial_candidato', 'sigla_partido')

# Faz o join dos eleitos e das informações (partido).
dados = codigos.join(informacoes, codigos['sq_candidato'] == informacoes['sequencial_candidato'], 'inner')

# Gera um DataFrame com os dados dos pibs.
pibsDF = spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="eleicoes", table="pibs").\
    load()

# Obtém os PIBS de 2011.
pibs = pibsDF.\
    filter(pibsDF['ano'] == 2011).\
    select('municipio', 'valor').\
    distinct()

# Faz o join dos eleitos e das informações.
dados = codigos.join(pibs, codigos['codigo_municipio'] == pibs['municipio'], 'inner')

# Obtém as respostas desejadas (conta as entradas para cada coluna).
res_sexo = dados.filter(dados['valor'] > )

# Armazena o resultado no HDFS.
res_sexo.write.format("csv").save("hdfs://dricardo-master:9000/user/engdados/res_sexo.csv")

print 'Encerrado com sucesso.'
