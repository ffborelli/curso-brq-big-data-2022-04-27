#!/bin/bash
###################################
#
# segundo-script.sh
#
# Autor: Fabrizio
# Data de crição: 04 de maio de 2022
#
# Descrição: Busca um texto em um arquivo
# primeiro argumento e o nome do arquivo e o segundo, o texto a ser localizado
################################## 

if [ $# -le  1 ]
then 
    echo "Nome do arquivo e texto obrigatorio"
    exit 1
fi
# -i e case insensitive
CONTADOR=$(grep -ic -n $2.* $1)
echo "CONTADOR $CONTADOR"   

if [ $CONTADOR -eq 0 ]
then
    echo "Nenhuma ocorrencia da palavra"
elif [ $CONTADOR -eq 1 ]
then
    echo "Encontrou-se 1 ocorrencia"
else 
    echo "Encontrou-se $CONTADOR ocorrencia(s)"
fi