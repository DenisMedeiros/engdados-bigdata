from pyspark.sql import SparkSession

spark_candidatos = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .getOrCreate()

spark_resultados = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .getOrCreate()

df_candidatos = spark_candidatos.read.format("com.mongodb.spark.sql.DefaultSource").load()
df_candidatos.printSchema()
