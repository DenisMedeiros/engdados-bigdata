from pyspark.sql import SparkSession

SEXO = {
    2: 'Masculino', 
    4: 'Feminino'
}

ESCOLARIDADE = {
    2: 'Lê e Escreve',
    3: 'Ensino Fundamental Incompleto',
    4: 'Ensino Fundamental Completo',
    5: 'Ensino Médio Incompleto',
    6: 'Ensino Médio Completo',
    7: 'Superior Incompleto',
    8: 'Superior Completo',
}

ETNIA = {
    1: 'Branca',
    2: 'Preta',
    3: 'Parda',
    4: 'Amarela',
    5: 'Indígena',
}

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

# Obtém as respostas desejadas.
res_sexo = dados.groupby(['codigo_sexo']).count()

res_sexo.rdd.map(lambda row: (row['codigo_sexo'], (SEXO[row['codigo_sexo']], row['count'])))

res_grau_instrucao = dados.groupby(['cod_grau_instrucao']).count()
res_cor = dados.groupby(['cor']).count()
res_despesa = dados.groupby(['despesa_max_campanha']).count()




'''
# Obtém os resultados por sexo.
resultado_sexo = []
for key, value in SEXO.iteritems():
    resultado_sexo.append('{}: ')

qnt_eleitos_m = dados.filter(dados['codigo_sexo'] == 2).count()
qnt_eleitos_f = dados.filter(dados['codigo_sexo'] == 2).count()

# Obtém os resultados por grau de instrucao.
'''

