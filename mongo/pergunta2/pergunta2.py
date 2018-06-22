#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

# Cria a sessão.
spark = SparkSession.builder.appName("Pergunta2").getOrCreate()


spark_eleicoes = spark \
    .builder \
    .appName("eleicoesApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .getOrCreate()


# Gera um DataFrame com os resultados da eleição de 2014.
eleicoesDF = spark_eleicoes.read.format("com.mongodb.spark.sql.DefaultSource").load()


# Obtém o código dos candidatos eleitos e suas cidades.
codigos = eleicoesDF.\
    filter(eleicoesDF['codigo_sit_cand_tot'] < 4).\
    select('sq_candidato', 'codigo_municipio').\
    distinct()

spark_candidatos= spark \
    .builder \
    .appName("candidatosApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .getOrCreate()

# Gera um DataFrame com os dados dos candidatos.
candidatosDF = spark_candidatos.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Obtém apenas as informações úteis.
informacoes = candidatosDF.select('sequencial_candidato', 'sigla_partido')


spark_pibs = spark \
    .builder \
    .appName("pibsApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.pibs") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.pibs") \
    .getOrCreate()

# Gera um DataFrame com os dados dos pibs.
pibsDF = spark_pibs.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Obtém os PIBS de acima de R$ 70.000,00;
pibs = pibsDF.\
    filter(pibsDF['pib_percapita'] > 70000.0).\
    select('uf', 'cidade', 'cod_tse')

# Faz o join dos eleitos e das informações (partido).
dados_eleitos = codigos.join(informacoes, codigos['sq_candidato'] == informacoes['sequencial_candidato'], 'inner')

# Faz o join dos eleitos e das informações.
final = dados_eleitos.join(pibs, dados_eleitos['codigo_municipio'] == pibs['cod_tse'], 'inner')

# Obtém as respostas desejadas (conta as entradas para cada coluna).
res_partidos = final.groupby(['sigla_partido']).count()

# Armazena o resultado no HDFS.
res_partidos.write.format("csv").save("hdfs://mtargino-master:9000/user/engdados/pergunta2/res_partidos.csv")

print 'Encerrado com sucesso.'
