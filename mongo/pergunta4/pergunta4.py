from pyspark.sql import SparkSession as spark

#configuração do spark
spark.builder.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0' ).getOrCreate()


#conexão com a collection
spark_eleitores = spark \
    .builder \
    .appName("eleitoresApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.eleitores") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.eleitores") \
    .getOrCreate()

df_eleitores = spark_eleitores.read.format("com.mongodb.spark.sql.DefaultSource").load()
df_informacoes = df_eleitores.select('uf', 'faixa_etaria', 'grau_de_escolaridade', 'qtd_eleitores_no_perfil')

#seleciona informações úteis
total_eleitores = df_eleitores.groupby(['uf']).count().withColumnRenamed('count', 'total')
#obtem eleitores facultativos
df_eleitores_facultativos = df_informacoes.filter((df_informacoes['faixa_etaria'] == '16 ANOS') | \
 (df_informacoes['faixa_etaria'] == '17 ANOS') | \
 (df_informacoes['faixa_etaria'] == '70 A 79 ANOS') | \
 (df_informacoes['faixa_etaria'] == '17 ANOS') | \
 (df_informacoes['faixa_etaria'] == 'SUPERIOR A 79 ANOS') | \
 (df_informacoes['grau_de_escolaridade'] == 'ANALFABETO'))

# Obtém as respostas desejadas (conta as entradas para cada coluna).
total_facultativos = df_eleitores_facultativos.groupby(['uf']).count()

# Encontra o valor relativo de eleitores.
final = total_eleitores.join(total_opcionais, ['uf'], 'inner')

# Calcula a resposta (valores percentuais dos eleitores).
res_eleitores = final.withColumn("relativo", final["count"] / final["total"])
# Armazena o resultado no HDFS.
res_eleitores.write.format("csv").save("hdfs://mcruz-master:9000/user/engdados/pergunta4/res_eleitores.csv")

print('Encerrado com sucesso')
