#!/bin/bash

docker run --rm -d --network singlenode_default --name consumer consume 

docker run --rm -d --network singlenode_default   --name producer producer 