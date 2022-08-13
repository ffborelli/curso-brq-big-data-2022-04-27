#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("otp-python").getOrCreate();

df = spark.read.csv('Bovespa.csv', inferSchema=True, header=True)

df.show(3)

dfGroupBy = df.groupBy(df.Company).sum()

dfGroupBy.show(5)


print("Finish")


#!/bin/bash
set -e

log()
{
    log_level="${1}"
    log_message="${2}"
    echo -e `date "+%Y-%m-%d %H:%M:%S"` " - [${log_level}] - main_shell - " "${log_message}"
}

run_kinit()
{
    log DEBUG "Getting kerberos ticket."
    kinit -kt ${KERBEROS_KEYTAB_PATH} ${KERBEROS_USERNAME}
    log INFO "User authenticated in Kerberos."
}

sparkSubmit()
{
    # Load spark configurations set by user per environment
    log INFO "Environment: $ENVIRONMENT"
    . ./spark-configs/spark-defaults-$ENVIRONMENT.conf

    log DEBUG 'Running spark-submit.'
    log DEBUG "spark-submit --master yarn \
                --deploy-mode $DEPLOY_MODE \
                --driver-cores $DRIVER_CORES \
                --driver-memory $DRIVER_MEMORY \
                --executor-cores $EXECUTOR_CORES \
                --num-executors $NUM_EXECUTORS \
                    --queue $QUEUE_NAME \
                --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PACKED_VENV \
                --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=$PACKED_VENV \
                --name $COMPONENT_NAME \
                
--archives ${COMPONENT_NAME}-venv.tar.gz#environment \
      --py-files src/RendaRiscos/funcoesRR.py,src/RendaRiscos/RendaRiscos.py, src/funcoesEstruturais.py, src/SourcePolitica.py\
      ./src/runPolitica.py "@"
}

run_kinit
sparkSubmit "@"

./src/runPolitica.py 1 2 3 4 5

$@ -> 1 2 3 4 5
