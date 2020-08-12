#!/usr/bin/env bash

lr_transaction_csv_to_json.py \
    --input ../data/sample/pp-monthly-update-new-version.csv \
    --output ../data/transformed_test_data.json --errors \
    ../data/transformed_test_data_errors.json
