
#!/bin/bash
###################################
#
# quarto-script.sh
#
# Autor: Fabrizio
# Data de crição: 04 de maio de 2022
#
# Descrição: Estrutura de repeticao WHILE
##################################
CONTADOR=0

read -p "Informe um numero: " NUMERO
echo "NUMERO: $NUMERO"

while [ $CONTADOR -le $NUMERO ]
do
    echo "$CONTADOR" 
    sleep 1
    CONTADOR=$(($CONTADOR + 1))

    echo "Num processos atuais: $(ps aux | wc -l)"

done
