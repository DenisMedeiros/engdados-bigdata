#!/bin/bash

# Comando a ser executado

COMMAND="python data-read-candidatos.py"

# Mede o tempo de execução em milissegundos.

START_TIME=$(date +%s%N)

eval $COMMAND

END_TIME=$(date +%s%N)
ELAPSED_TIME=$(($END_TIME - $START_TIME))

# Converte para milissegundos.
ELAPSED_TIME=$(( ELAPSED_TIME/1000000 ))

# Salva a resposta.
echo "Tempo total do comando '$COMMAND': $ELAPSED_TIME ms." >> ./resultado-medicao.txt

exit 0
