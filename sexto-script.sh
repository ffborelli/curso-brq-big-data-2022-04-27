#!/bin/bash
###################################
#
# sexto-script.sh
#
# Autor: Fabrizio
# Data de crição: 04 de maio de 2020

# Descricao: mostrar recursos computacionais
##################################

HOSTNAME=$(hostname)


while true
do

    DATET=$(date "+%Y-%m-%d %H:%M:%S")
    CPUUSAGE=$(top -b -n 2 -d1 | grep "Cpu(s)" | tail -n1 | awk '{print $2}' |awk -F. '{print $1}')
    MEMUSAGE=$(free | grep Mem | awk '{print $3/$2 * 100.0}')
    DISKUSAGE=$(df -h / | awk '{print $5}' |tail -n 1 |sed 's/%//g')

    echo $DATET
    echo "HOSTNANE: $HOSTNAME"
    echo "CPU: $CPUUSAGE%"
    echo "MEMORIA: $MEMUSAGE%"
    echo "DISCO: $DISKUSAGE%" 
    echo "======================"

    sleep 5
done