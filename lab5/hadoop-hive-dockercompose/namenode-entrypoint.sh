#!/bin/bash
set -e

NN_DIR=/hadoop/dfs/name

echo "Checking NameNode metadata in $NN_DIR..."

if [ -f $NN_DIR/current/VERSION ]; then
    echo "Existing HDFS metadata found. Checking layout version..."
    grep -q "layoutVersion=-65" $NN_DIR/current/VERSION || {
        echo "Old layout version found! Reformatting NameNode..."
        rm -rf $NN_DIR/*
        hdfs namenode -format -nonInteractive -force -clusterId CID-$(date +%s)
    }
else
    echo "No existing NameNode data found. Formatting fresh NameNode..."
    hdfs namenode -format -nonInteractive -force -clusterId CID-$(date +%s)
fi

# Start the official Hadoop entrypoint
exec /entrypoint.sh "$@"
