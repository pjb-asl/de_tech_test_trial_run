#!/usr/bin/env bash


python pipelines/lr_transaction_csv_to_json.py \
    --input gs://data_daddy/pp-monthly-update-new-version.csv \
    --output gs://data_daddy/trns_stage_out.json \
    --errors gs://data_daddy/trans_stage_errors.json \
    --runner DataflowRunner \
    --project streetgroupdatdscience \
    --region europe-west2 \
    --temp_location gs://dataflow_temp_streetgroupds/temp

