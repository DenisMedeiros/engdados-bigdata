#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
from pyspark_cassandra import CassandraSparkContext

# Cria o SparkContext
conf = SparkConf() \
	.setAppName("Pergunta 1") \
	.set("spark.cassandra.connection.host", "localhost")
csc = CassandraSparkContext(conf=conf)

# Prepara os RDDs das tabelas.
candidatosRDD = csc.cassandraTable("eleicoes", "candidatos2014")
resultadosRDD = csc.cassandraTable("eleicoes", "resultados2014")

# Faz as buscas.

cod_sit_cand_tot

eleitos = resultadosRDD.select("sq_candidato").where("cod_sit_cand_tot=?", 1)

.where("key=?", "x") \
	.filter(lambda r: r["col-b"].contains("foo")) \
	.map(lambda r: (r["col-a"], 1)
	.reduceByKey(lambda a, b: a + b)
	.collect()

