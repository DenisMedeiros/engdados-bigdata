from pyspark.sql import SparkSession

# Cria a sessão.
spark = SparkSession.builder.appName("Pergunta1").getOrCreate()

# Gera um DataFrame com os resultados da eleição de 2014.
eleicoesDF = spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="eleicoes", table="resultados2014").\
    load()

# Obtém o código dos candidatos eleitos.
codigos = eleicoesDF.\
    filter(eleicoesDF['codigo_sit_cand_tot'] < 4).\
    select('sq_candidato').\
    distinct()

# Gera um DataFrame com os dados dos candidatos.
candidatosDF = spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="eleicoes", table="candidatos2014").\
    load()

# Obtém apenas as informações úteis.
informacoes = candidatosDF.select('sequencial_candidato', 'codigo_sexo', 'cod_grau_instrucao', 'codigo_cor_raca', 'despesa_max_campanha')

# Faz o join dos eleitos e das informações.
dados = codigos.join(informacoes, codigos['sq_candidato'] == informacoes['sequencial_candidato'], 'inner')

# Obtém as respostas desejadas (conta as entradas para cada coluna).
res_sexo = dados.groupby(['codigo_sexo']).count().collect()
res_grau_instrucao = dados.groupby(['cod_grau_instrucao']).count().collect()
res_cor = dados.groupby(['cor']).count().collect()
res_despesa = dados.groupby(['despesa_max_campanha']).count().collect()

# Armazena o resultado no HDFS.
res_sexo.write.format("csv").save("hdfs://dricardo-master:9000/user/engdados/res_sexo.csv")
res_grau_instrucao.write.format("csv").save("hdfs://dricardo-master:9000/user/engdados/res_grau_instrucao.csv")
res_cor.write.format("csv").save("hdfs://dricardo-master:9000/user/engdados/res_cor")
res_despesa.write.format("csv").save("hdfs://dricardo-master:9000/user/engdados/res_despesa")

print 'Encerrado com sucesso.'
