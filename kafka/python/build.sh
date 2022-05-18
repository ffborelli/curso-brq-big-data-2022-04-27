#!/bin/bash

docker build -t consumer -f Dockerfile-consumer .

docker build -t producer -f Dockerfile-producer .

