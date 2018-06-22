from pyspark.sql import SparkSession as spark

#configuração do spark
spark.builder.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0' ).getOrCreate()


#conexão com a collection de candidatos
spark_eleitores = spark \
    .builder \
    .appName("eleitoresApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.eleitores") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.eleitores") \
    .getOrCreate()

df_eleitores = spark_eleitores.read.format("com.mongodb.spark.sql.DefaultSource").load()


#seleciona informações úteis
df_informacoes = df_eleitores.select('uf', 'faixa_etaria', 'grau_de_escolaridade', 'qtd_eleitores_no_perfil')
df_analfabetos = df_informacoes.filter(df_informacoes['grau_de_escolaridade']=='ANALFABETO').groupBy('uf').sum('qtd_eleitores_no_perfil').orderBy('sum(qtd_eleitores_no_perfil)', ascending=False)
df_16 = df_informacoes.filter(df_informacoes['faixa_etaria']=='16 ANOS').groupBy('uf').sum('qtd_eleitores_no_perfil').orderBy('sum(qtd_eleitores_no_perfil)', ascending=False)
df_17 = df_informacoes.filter(df_informacoes['faixa_etaria']=='17 ANOS').groupBy('uf').sum('qtd_eleitores_no_perfil').orderBy('sum(qtd_eleitores_no_perfil)', ascending=False)
df_70 = df_informacoes.filter(df_informacoes['faixa_etaria']=='70 A 79 ANOS').groupBy('uf').sum('qtd_eleitores_no_perfil').orderBy('sum(qtd_eleitores_no_perfil)', ascending=False)
df_79 = df_informacoes.filter(df_informacoes['faixa_etaria']=='SUPERIOR A 79 ANOS').groupBy('uf').sum('qtd_eleitores_no_perfil').orderBy('sum(qtd_eleitores_no_perfil)', ascending=False)

df_informacoes.createOrReplaceTempView("info")
df_tot = spark.sql("SELECT uf FROM info \
WHERE faixa_etaria = '16 ANOS' \
OR faixa_etaria = '17 ANOS' \
OR faixa_etaria = '70 A 79 ANOS' \
OR faixa_etaria = 'SUPERIOR A 79 ANOS' \
OR grau_de_escolaridade = 'ANALFABETO' \
GROUP BY uf")

df_analfabetos_tot = df_informacoes.filter(df_informacoes['grau_de_escolaridade']=='ANALFABETO').groupBy().sum('qtd_eleitores_no_perfil')


# Obtém as respostas desejadas (conta as entradas para cada coluna).
res_sexo = dados.groupby(['codigo_sexo']).count()
res_grau_instrucao = dados.groupby(['cod_grau_instrucao']).count()
res_cor = dados.groupby(['codigo_cor_raca']).count()
res_despesa = dados.groupby(['despesa_max_campanha']).count()

# Armazena o resultado no HDFS.
res_sexo.write.format("csv").save("hdfs://mcruz-master:9000/user/engdados/res_sexo.csv")
res_grau_instrucao.write.format("csv").save("hdfs://mcruz-master:9000/user/engdados/res_grau_instrucao.csv")
res_cor.write.format("csv").save("hdfs://mcruz-master:9000/user/engdados/res_cor")
res_despesa.write.format("csv").save("hdfs://mcruz-master:9000/user/engdados/res_despesa")

print('Encerrado com sucesso')
