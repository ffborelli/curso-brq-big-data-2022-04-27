#!/bin/bash
###################################
#
# quinto-script.sh
#
# Autor: Fabrizio
# Data de crição: 04 de maio de 2022
#
# Descrição: Estrutura de repeticao FOR aplicado a pastas
##################################

for i in /home/virtual/Desktop/brq/*
do
    #echo $i
    # Se o arquivo existir
    if [ -f $i ]
    then
        echo "O arquvivo $i possui $(cat $i | wc -l) linhas"
    fi
done