#!/bin/bash
# Prerequisite: 
# 1. Hadoop and HDFS services should be running.
# 2. YARN Should be Running.
# 2. HDFS directory structure should be prepared if necessary.
# 3. Spark should be installed and configured properly.
# 4. you Can start the file by "bash Starter.sh"

source unset_jupyter.sh

# Define variables
PYSPARK_SCRIPT="london_property_analysis.py"  # PySpark script name
LOCAL_DATA_FILE="./dataset/london_house_price_data.csv"  # Local file path
HDFS_DATA_FILE="/user/$(whoami)/london_house_price_data.csv"  # HDFS file path
LOG_FILE="analysis_log.txt"  # Log file for script output

# Step 1: Check if the PySpark script exists
if [ ! -f "$PYSPARK_SCRIPT" ]; then
  echo "Error: PySpark script '$PYSPARK_SCRIPT' not found." >&2
  exit 1
fi

# Step 2: Check if the local data file exists
if [ ! -f "$LOCAL_DATA_FILE" ]; then
  echo "Error: Local data file '$LOCAL_DATA_FILE' not found." >&2
  exit 1
fi

# Step 3: Check if the file exists in HDFS
echo "Checking if data file exists in HDFS..."
hdfs dfs -test -e "$HDFS_DATA_FILE"
if [ $? -ne 0 ]; then
  echo "File not found in HDFS. Uploading '$LOCAL_DATA_FILE' to HDFS as '$HDFS_DATA_FILE'..."
  hdfs dfs -put "$LOCAL_DATA_FILE" "$HDFS_DATA_FILE"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to upload file to HDFS." >&2
    exit 1
  fi
else
  echo "File already exists in HDFS: $HDFS_DATA_FILE"
fi

# Step 4: Run the PySpark script
echo "Running PySpark script..."
spark-submit --master yarn "$PYSPARK_SCRIPT" > "$LOG_FILE" 2>&1

# Step 5: Check if the script ran successfully
if [ $? -eq 0 ]; then
  echo "PySpark script executed successfully. Check '$LOG_FILE' for details."
else
  echo "Error: PySpark script failed to execute. Check '$LOG_FILE' for errors." >&2
  exit 1
fi

# Step 6: Print completion message
echo "Automation script completed successfully!"