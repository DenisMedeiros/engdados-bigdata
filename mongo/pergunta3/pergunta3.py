from pyspark.sql import SparkSession as spark

#configuração do spark
spark.builder.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0' ).getOrCreate()

#ELEITORADO
#conexão com a collection
spark_eleitores = spark \
    .builder \
    .appName("eleitoresApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.eleitores") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.eleitores") \
    .getOrCreate()

df_eleitores = spark_eleitores.read.format("com.mongodb.spark.sql.DefaultSource").load()

#seleciona informações úteis
df_informacoes_eleitores = df_eleitores.select('cod_municipio_tse', 'grau_de_escolaridade', 'qtd_eleitores_no_perfil')

#obtém total de eleitores
total_eleitores = df_eleitores.groupby(['cod_municipio_tse']).sum('qtd_eleitores_no_perfil').withColumnRenamed('sum(qtd_eleitores_no_perfil)', 'total')

#obtém eleitores baixa escolaridade
df_eleitores_baixa_esc = df_informacoes_eleitores.filter((df_informacoes_eleitores['grau_de_escolaridade'] == 'ANALFABETO' )| \
 (df_informacoes_eleitores['grau_de_escolaridade'] == 'LÊ E ESCREVE') | \
 (df_informacoes_eleitores['grau_de_escolaridade'] == 'ENSINO FUNDAMENTAL INCOMPLETO'))

#obtém número total de eleitores com baixa escolaridade por municipio
total_baixa_esc = df_eleitores_baixa_esc.groupby('cod_municipio_tse').sum('qtd_eleitores_no_perfil').withColumnRenamed('sum(qtd_eleitores_no_perfil)', 'total_baixa_esc')

total_rel = total_eleitores.join(total_baixa_esc,['cod_municipio_tse'],'inner')

#valor relativo para cada categoria
rel_b = total_rel.withColumn('baixa_rel',total_rel['total_baixa_esc']/total_rel['total'])

rel = rel_b.filter(rel_b['baixa_rel'] > 0.5)

#------------------------------------------
#RESULTADOS
#conexão com a collection
spark_resultados = spark \
    .builder \
    .appName("resultadosApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.resultados2014") \
    .getOrCreate()

df_resultados = spark_resultados.read.format("com.mongodb.spark.sql.DefaultSource").load()

#seleciona informações úteis
df_informacoes_resultados = df_resultados.select('codigo_municipio','nome_municipio','sq_candidato','total_votos')

#------------------------------------------
#CANDIDATOS
#conexão com a collection
spark_candidatos = spark \
    .builder \
    .appName("candidatosApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.54/eleicoes.candidatos2014") \
    .getOrCreate()

df_candidatos = spark_candidatos.read.format("com.mongodb.spark.sql.DefaultSource").load()

#seleciona informações úteis
df_informacoes_candidatos = df_candidatos.select('sequencial_candidato','sigla_partido','nome_partido')

#juncao das tabelas de resultados e candidatos
df_res_can = df_informacoes_resultados.join(df_informacoes_candidatos, \
df_informacoes_resultados['sq_candidato']==df_informacoes_candidatos['sequencial_candidato'],\
'inner')
#partido mais votado de cada municipio
df_partido_municipio = df_res_can.groupBy('nome_municipio','codigo_municipio','sigla_partido').sum('total_votos').withColumnRenamed('sum(total_votos)', 'total_votos').orderBy('total_votos',ascending=False)

#obtém tabela geral com todas as informações necessárias
df_geral = df_partido_municipio.join(rel,rel['cod_municipio_tse']== df_partido_municipio['codigo_municipio'],'inner')

#------------------------------------------
#RESPOSTAS
# Calcula a resposta (valores percentuais dos eleitores).
df_baixa_esc = df_geral.select('sigla_partido','total_votos').groupby('sigla_partido').sum('total_votos').withColumnRenamed('sum(total_votos)', 'total_votos').orderBy('total_votos', ascending=False)

# Armazena o resultado no HDFS.
df_baixa_esc.format("csv").save("hdfs://mcruz-master:9000/user/engdados/pergunta3/res_baixa_esc.csv")
print('Encerrado com sucesso')
