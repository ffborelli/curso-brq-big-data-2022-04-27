#!/usr/bin/env bash

# for file path
DIR="$(cd "$(dirname "$0")" && pwd -P)"

psql -Upostgres -d pipeline_db -a -f $DIR/../resources/sql/create_tables.sql 
