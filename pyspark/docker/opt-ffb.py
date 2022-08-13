#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("otp-python").getOrCreate();

df = spark.read.csv('Bovespa.csv', inferSchema=True, header=True)

df.show(3)

dfGroupBy = df.groupBy(df.Company).sum()

dfGroupBy.show(5)


print("Finish")
