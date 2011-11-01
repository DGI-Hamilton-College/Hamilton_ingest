#!/bin/bash

echo "Calling ingester: breaking TEI's"
#might need to cd into top level

mkdir ./logs 

python ./break_TEIs.py ./tei-xml ./tei-xml/pages &> ./logs/break_TEIs_report.txt

echo "Running letter ingester: ingest to fedora"

python ./letter_ingest.py ./ &> ./logs/letter_ingest_report.txt

echo "Done."