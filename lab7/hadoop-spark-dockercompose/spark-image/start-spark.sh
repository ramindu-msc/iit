#!/bin/bash

# Default behavior: start Spark master, worker, or notebook
if [ "$SPARK_MODE" == "master" ]; then
    /opt/spark/sbin/start-master.sh -h 0.0.0.0
elif [ "$SPARK_MODE" == "worker" ]; then
    if [ -z "$SPARK_MASTER_URL" ]; then
        echo "SPARK_MASTER_URL must be set for worker mode."
        exit 1
    fi
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
elif [ "$SPARK_MODE" == "notebook" ]; then
    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
else
    echo "Invalid SPARK_MODE. Set SPARK_MODE to 'master', 'worker', or 'notebook'."
    exit 1
fi

# Keep container alive
tail -f /dev/null
