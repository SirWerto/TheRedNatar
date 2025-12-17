#!/usr/bin/bash 


REPO_FOLDER_PATH=$1
TRAVIAN_SNAPSHOT_FOLER_PATH=$2
mkdir -p $TRAVIAN_SNAPSHOT_FOLER_PATH

PRIV_PATH=$REPO_FOLDER_PATH/priv
KERNELS_PATH=$REPO_FOLDER_PATH/kernels
SCRIPTS_PATH=$REPO_FOLDER_PATH/scripts


TMP_FOLDER=$(mktemp --directory)

echo "Start collection"

echo "Create env"
python -m venv $TMP_FOLDER/venv
source $TMP_FOLDER/venv/bin/activate
pip install -r $REPO_FOLDER_PATH/requirements.txt

echo "1 - Fetch server list"
SERVER_LIST_FILE_PATH=$TMP_FOLDER/servers.csv
time sh $SCRIPTS_PATH/generate_travian_subdomain_list.sh $SERVER_LIST_FILE_PATH

echo "2 - Push server list to Kaggle"
sh $SCRIPTS_PATH/travian_subdomain_list_to_kaggle.sh $SERVER_LIST_FILE_PATH $PRIV_PATH/travian_subdomain_list.json

echo "3 - Collect server snapshots"
SNAPSHOT_FILE_PATH=$TRAVIAN_SNAPSHOT_FOLER_PATH/$(date --rfc-3339=date).parquet
time python $SCRIPTS_PATH/clean_raw_map_sql.py $SERVER_LIST_FILE_PATH $SNAPSHOT_FILE_PATH

echo "4 - Push snapshots to Kaggle"
sh $SCRIPTS_PATH/snapshot_to_kaggle.sh $TRAVIAN_SNAPSHOT_FOLER_PATH $PRIV_PATH/travian_map_sql_snapshot.json

echo "5 - Run notebook travian-base-star-schema"
kaggle kernels push --path $KERNELS_PATH/travian-base-star-schema/

deactivate
rm -r $TMP_FOLDER
