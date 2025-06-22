#!/usr/bin/env bash

# Fail fast 
set -e

# Input and output paths
INPUT_FILE="../data/boat_data.csv"
OUTPUT_DIR="../output"

# Make sure output directory exists
mkdir -p $OUTPUT_DIR

# Run the pipeline
python3 ../src/pipeline_pyspark.py -i $INPUT_FILE -o $OUTPUT_DIR -s $OUTPUT_DIR

echo "==> Parquet: $OUTPUT_DIR/data.parquet"
echo "==> Summary CSV: $OUTPUT_DIR/data_summary.csv"