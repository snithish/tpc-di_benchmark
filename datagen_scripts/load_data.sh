#!/usr/bin/env sh

# Usage: ./load_data.sh $BATCH_ID

BATCH_ID=$1
BATCH_PATH="${HOME}/data/Batch${BATCH_ID}"

if [ "${BATCH_ID}" -eq 1 ]; then
  gsutil -m cp -r "${BATCH_PATH}" gs://tpc-di_data/historical
else
  gsutil -m cp -r "${BATCH_PATH}" gs://tpc-di_data/incremental
fi
