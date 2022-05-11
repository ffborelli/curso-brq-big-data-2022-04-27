#!/bin/bash
###################################
#
# terceiro-script.sh
#
# Autor: Fabrizio
# Data de crição: 04 de maio de 2022
#
# Descrição: Estrutura de repeticao FOR
##################################

#for i in 1 2 3 4 5
#for i in $(seq 1 5)
# inicia, incremento, final
for i in $(seq 1 2 10) 
do
    echo "$i"
done

#for counter in {1..255}; do ping -c 1 10.0.0.$counter; done

