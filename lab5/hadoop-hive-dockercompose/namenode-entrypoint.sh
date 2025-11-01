#!/bin/bash
set -e

NN_DIR=/hadoop/dfs/name

# Only format if the directory is empty
if [ ! -d "$NN_DIR/current" ]; then
    echo "No existing NameNode data found. Formatting fresh NameNode..."
    hdfs namenode -format -nonInteractive -force -clusterId CID-$(date +%s)
else
    echo "Existing HDFS metadata found. Skipping format."
fi

# Start the official Hadoop entrypoint (this will run NameNode in foreground)
exec /entrypoint.sh "$@"
