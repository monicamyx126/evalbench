#!/bin/bash

ZIP_URL="https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip"
ZIP_FILE="dev.zip"
EXTRACT_DIR="datasets/bird/data"
DB_CONNECTIONS_DIR="db_connections/bird"
PROMPTS_FILE="datasets/bird/prompts.json"

extract_zip() {
    local zip_path=$1
    local extract_to=$2
    
    python3 -c "
import zipfile
import os

zip_path = '$zip_path'
extract_to = '$extract_to'

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    members = [m for m in zip_ref.namelist()]
    zip_ref.extractall(extract_to, members)
"
}

curl -o "$ZIP_FILE" -L "$ZIP_URL"

if [ ! -f "$ZIP_FILE" ]; then
    echo "Error: Failed to download the zip file!"
    exit 1
fi

extract_zip "$ZIP_FILE" "$EXTRACT_DIR"
rm -f "$ZIP_FILE"

EXTRACTED_DIR=$(ls "$EXTRACT_DIR" | grep '^dev_' | head -n 1)
if [ -z "$EXTRACTED_DIR" ]; then
    echo "Error: No extracted directory found!"
    exit 1
fi

EXTRACTED_PATH="$EXTRACT_DIR/$EXTRACTED_DIR"
DEV_DB_ZIP="$EXTRACTED_PATH/dev_databases.zip"

if [ -f "$DEV_DB_ZIP" ]; then
    extract_zip "$DEV_DB_ZIP" "$EXTRACTED_PATH"
    rm -f "$DEV_DB_ZIP"
else
    echo "Warning: $DEV_DB_ZIP not found!"
fi

mkdir -p "$DB_CONNECTIONS_DIR"
find "$EXTRACTED_PATH/dev_databases" -name "*.sqlite" -exec mv {} "$DB_CONNECTIONS_DIR/" \;

touch "$PROMPTS_FILE"
if [ -f "$EXTRACTED_PATH/dev.json" ]; then
    mv "$EXTRACTED_PATH/dev.json" "$PROMPTS_FILE"
else
    echo "Warning: dev.json not found!"
fi

rm -rf "$EXTRACT_DIR"
echo "Script execution completed successfully."
