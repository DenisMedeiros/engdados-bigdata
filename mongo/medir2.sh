#!/bin/bash

# Comando a ser executado

COMMAND="spark-submit --deploy-mode client --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 pergunta1/pergunta1.py"

# Mede o tempo de execução em milissegundos.

START_TIME=$(date +%s%N)

eval $COMMAND

END_TIME=$(date +%s%N)
ELAPSED_TIME=$(($END_TIME - $START_TIME))

# Converte para milissegundos.
ELAPSED_TIME=$(( ELAPSED_TIME/1000000 ))

# Salva a resposta.
echo "Tempo total do comando '$COMMAND': $ELAPSED_TIME ms." >> ./resultado-medicao-SPARK.txt

# Comando a ser executado

COMMAND="spark-submit --deploy-mode client --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 pergunta2/pergunta2.py"

# Mede o tempo de execução em milissegundos.

START_TIME=$(date +%s%N)

eval $COMMAND

END_TIME=$(date +%s%N)
ELAPSED_TIME=$(($END_TIME - $START_TIME))

# Converte para milissegundos.
ELAPSED_TIME=$(( ELAPSED_TIME/1000000 ))

# Salva a resposta.
echo "Tempo total do comando '$COMMAND': $ELAPSED_TIME ms." >> ./resultado-medicao-SPARK.txt

# Comando a ser executado

COMMAND="spark-submit --deploy-mode client --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 pergunta3/pergunta3.py"

# Mede o tempo de execução em milissegundos.

START_TIME=$(date +%s%N)

eval $COMMAND

END_TIME=$(date +%s%N)
ELAPSED_TIME=$(($END_TIME - $START_TIME))

# Converte para milissegundos.
ELAPSED_TIME=$(( ELAPSED_TIME/1000000 ))

# Salva a resposta.
echo "Tempo total do comando '$COMMAND': $ELAPSED_TIME ms." >> ./resultado-medicao-SPARK.txt

# Comando a ser executado

COMMAND="spark-submit --deploy-mode client --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 pergunta4/pergunta4.py"

# Mede o tempo de execução em milissegundos.

START_TIME=$(date +%s%N)

eval $COMMAND

END_TIME=$(date +%s%N)
ELAPSED_TIME=$(($END_TIME - $START_TIME))

# Converte para milissegundos.
ELAPSED_TIME=$(( ELAPSED_TIME/1000000 ))

# Salva a resposta.
echo "Tempo total do comando '$COMMAND': $ELAPSED_TIME ms." >> ./resultado-medicao-SPARK.txt
