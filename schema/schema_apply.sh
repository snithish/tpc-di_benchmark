#!/bin/bash

# Create schema for staging and master dataset tables

STG_DS=staging
MASTER_DS=master

bq rm -f $STG_DS && bq mk $STG_DS
bq rm -f $MASTER_DS && bq mk $MASTER_DS

cat schema/staging/create_table.sql | bq query --use_legacy_sql=false --batch=false