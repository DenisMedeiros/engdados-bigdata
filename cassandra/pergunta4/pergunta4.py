#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

# Cria a sessão.
spark = SparkSession.builder.appName("Pergunta4").getOrCreate()

# Gera um DataFrame com os dados dos eleitores.
eleitoresDF = spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="eleicoes", table="eleitores").\
    load()

# Obtém apenas as informações úteis dos eleitores.
informacoes = eleitoresDF.select('uf', 'faixa_etaria', 'grau_de_escolaridade', 'qtd_eleitores_no_perfil')

# Total eleitores por estado.
total_eleitores = eleitoresDF.groupby(['uf']).count().withColumnRenamed('count', 'total')

# Filtra os candidatos nas faixas etárias opcionais.
opcionais = informacoes.filter((informacoes['faixa_etaria'] == '16 ANOS') | (informacoes['faixa_etaria'] == '17 ANOS') | (informacoes['faixa_etaria'] == '70 A 79 ANOS') | (informacoes['faixa_etaria'] == '17 ANOS') | (informacoes['faixa_etaria'] == 'SUPERIOR A 79 ANOS') | (informacoes['grau_de_escolaridade'] == 'ANALFABETO'))

# Obtém as respostas desejadas (conta as entradas para cada coluna).
total_opcionais = opcionais.groupby(['uf']).count()

# Encontra o valor relativo de eleitores.
final = total_eleitores.join(total_opcionais, ['uf'], 'inner')

# Calcula a resposta (valores percentuais dos eleitores).
res_eleitores = final.withColumn("relativo", final["count"] / final["total"])

# Armazena o resultado no HDFS.
res_eleitores.write.format("csv").save("hdfs://dricardo-master:9000/user/engdados/res_eleitores.csv")

print 'Encerrado com sucesso.'
