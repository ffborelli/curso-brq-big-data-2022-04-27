#!/bin/bash

docker run --rm -d --network singlenode_default --name consumer consumer 

docker run --rm -d --network singlenode_default   --name producer producer 