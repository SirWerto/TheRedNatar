#!/usr/bin/bash 

TRAVIAN_SNAPSHOT_FOLER_PATH=$1
METADATA_FILE_PATH=$2


cp $METADATA_FILE_PATH $TRAVIAN_SNAPSHOT_FOLER_PATH/dataset-metadata.json

kaggle datasets version --path $TRAVIAN_SNAPSHOT_FOLER_PATH --message "$(date)"
