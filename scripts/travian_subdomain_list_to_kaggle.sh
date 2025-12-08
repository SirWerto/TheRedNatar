#!/usr/bin/bash 

TRAVIAN_SUBDOMAIN_LIST_PATH=$1
METADATA_FILE_PATH=$2

TMP_FOLDER=$(mktemp --directory)

cp $TRAVIAN_SUBDOMAIN_LIST_PATH $TMP_FOLDER/travian_subdomain_list.csv
cp $METADATA_FILE_PATH $TMP_FOLDER/dataset-metadata.json
ls -la $TMP_FOLDER

kaggle datasets version --path $TMP_FOLDER --message "$(date)"

rm -r $TMP_FOLDER
