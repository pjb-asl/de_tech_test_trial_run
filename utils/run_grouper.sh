#!/usr/bin/env bash


python pipelines/property_transaction_grouper.py \
    --input gs://data_daddy/trns_stage_out.json \
    --output gs://data_daddy/property_trns_history_out.json \
    --errors gs://data_daddy/property_trans_history_errors.json \
    --runner DataflowRunner \
    --project streetgroupdatascience \
    --region europe-west2 \
    --temp_location gs://dataflow_temp_streetgroupds/temp