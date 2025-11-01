#!/bin/bash
# wait-for-it.sh
# Usage: ./wait-for-it.sh host port -- command_to_run

set -e

host="$1"
port="$2"
shift 2

# Optional: command to run after waiting
cmd="$@"

echo "Waiting for $host:$port to be available..."
while ! nc -z "$host" "$port"; do
  sleep 2
done

echo "$host:$port is available. Starting command..."
exec $cmd