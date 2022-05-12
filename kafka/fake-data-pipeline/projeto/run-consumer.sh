#!/usr/bin/env bash


sbt package;

spark-submit \
  --class Consumer \
  --master yarn \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" \
  target/scala-2.11/consumer_2.11-1.0.jar
