#!/bin/bash

# Define paths to necessary commands
DUCKDB=duckdb
SPARK=spark-submit

# Define paths to scripts
AIRBNB_CLEANER=code/airbnb_cleaning.py
YELP_CLEANER=code/yelp_cleaning.py
DUCKDB_QUERIES=code/queries.sql

# Define input data paths
AIRBNB_RAW=data/listings17.csv
YELP_RAW=data/yelp1.json

# Define output directories and files
OUTPUT_DIR=output
CLEANED_AIRBNB=$OUTPUT_DIR/airbnb_cleaned.csv
CLEANED_YELP=$OUTPUT_DIR/yelp_cleaned.csv
FINAL_OUTPUT=$OUTPUT_DIR/airbnb_yelp_distances.csv

# Rollback function in case of failure
rollback() {
    echo "Rolling back due to failure..."
    rm -rf $OUTPUT_DIR
}

# Function to display messages
message() {
    printf "%50s\n" | tr " " "-"
    printf "$1\n"
    printf "%50s\n" | tr " " "-"
}

# Function to check success or failure of commands
check() {
    if [ $? -eq 0 ]; then
        message "$1"
    else
        message "$2"
        rollback
        exit 1
    fi
}

# Run Spark jobs for data cleaning
run_spark() {
    rm -rf $OUTPUT_DIR
    mkdir -p $OUTPUT_DIR

    $SPARK $AIRBNB_CLEANER $AIRBNB_RAW $CLEANED_AIRBNB
    check "Airbnb data cleaned successfully." "Airbnb data cleaning FAILED."

    $SPARK $YELP_CLEANER $YELP_RAW $CLEANED_YELP
    check "Yelp data cleaned successfully." "Yelp data cleaning FAILED."
}

# Run DuckDB queries
run_duckdb() {
    $DUCKDB -no-stdin -csv < $DUCKDB_QUERIES > $FINAL_OUTPUT
    check "DuckDB query executed successfully." "DuckDB query FAILED."
}

# Pipeline execution starts here
message "Starting Data Processing Pipeline..."

run_spark
run_duckdb

check "Pipeline execution completed successfully." "Pipeline Failed"

