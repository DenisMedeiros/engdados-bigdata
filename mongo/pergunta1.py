from pyspark.sql import SparkSession

#configuração do spark
SparkSession.builder.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0' ).getOrCreate()

#conexão com a collection de candidatos
spark_candidatos = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .getOrCreate()

#conexão com a collection de resultados
spark_resultados = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .getOrCreate()

#leitura dos dados dos candidatos
df_candidatos = spark_candidatos.read.format("com.mongodb.spark.sql.DefaultSource").load()
df_candidatos.printSchema()
