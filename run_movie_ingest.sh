#!/bin/bash

#might need to cd into top level

mkdir ./logs 

echo "Running movie ingester: ingest to fedora"

python ./movie_ingest.py ./ > ./logs/movie_ingest_report.txt

echo "Done."