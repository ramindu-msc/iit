#!/bin/bash
set -e

# Path to HDFS NameNode data
NAMENODE_DIR="/hadoop/dfs/name"

echo "Checking NameNode metadata in $NAMENODE_DIR..."

if [ -f "$NAMENODE_DIR/current/VERSION" ]; then
    echo "Existing HDFS metadata found. Checking layout version..."
    if ! grep -q "layoutVersion=-65" "$NAMENODE_DIR/current/VERSION"; then
        echo "Old layout version detected! Reformatting NameNode..."
        rm -rf "$NAMENODE_DIR"/*
        hdfs namenode -format -nonInteractive -force -clusterId CID-$(date +%s)
    else
        echo "Layout version is up-to-date. Skipping format."
    fi
else
    echo "No existing NameNode data found. Formatting fresh NameNode..."
    hdfs namenode -format -nonInteractive -force -clusterId CID-$(date +%s)
fi

echo "Starting NameNode..."
# Call the original entrypoint from the image
exec /entrypoint.sh
