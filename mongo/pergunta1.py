#from pyspark.sql import SparkSession as spark

#configuração do spark
#spark.builder.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0' ).getOrCreate()


#-------------------------------------------------------------------------
#COMANDOS PARA RODAR NA SHELL DO PYSPARK

#conexão com a collection de candidatos
spark_candidatos = spark \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .getOrCreate()

#conexão com a collection de resultados
spark_resultados = spark \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .getOrCreate()

#leitura dos dados dos candidatos
df_candidatos = spark_candidatos.read.format("com.mongodb.spark.sql.DefaultSource").load()
df_resultados = spark_resultados.read.format("com.mongodb.spark.sql.DefaultSource").load()


rdd_candidatos = df_candidatos.rdd
rdd_resultados = df_resultados.rdd


#cria tabela temporaria com os dados
df_candidatos.createOrReplaceTempView("temp_candidatos")
df_resultados.createOrReplaceTempView("temp_resultados")

query = spark.sql("SELECT tc.SIGLA_UF, SEQUENCIAL_CANDIDATO ,DESCRICAO_COR_RACA, DESCRICAO_SEXO, DESCRICAO_GRAU_INSTRUCAO, DESPESA_MAX_CAMPANHA, TOTAL_VOTOS FROM temp_candidatos as tc \
INNER JOIN temp_resultados AS tr ON tc.SEQUENCIAL_CANDIDATO = tr.SQ_CANDIDATO \
WHERE tr.DESC_SIT_CAND_TOT = 'ELEITO' \
ORDER BY TOTAL_VOTOS DESC \
LIMIT 20")

query.show()
