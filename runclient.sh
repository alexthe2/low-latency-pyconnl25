#!/bin/bash
# Usage: ./runclient 100@400
# Meaning: run 100 instances, each sending 400 messages per second

set -e

if [[ -z "$1" ]]; then
  echo "Usage: $0 INSTANCES@RATE"
  exit 1
fi

# Split the input like "100@400"
IFS='@' read -r INSTANCES RATE <<< "$1"

if ! [[ "$INSTANCES" =~ ^[0-9]+$ && "$RATE" =~ ^[0-9]+$ ]]; then
  echo "Error: argument must be in the form INSTANCES@RATE, e.g. 100@400"
  exit 1
fi

echo "Starting $INSTANCES instances at $RATE messages per second each..."

# Make sure all children die on Ctrl+C
trap 'kill 0' SIGINT

for i in $(seq 1 "$INSTANCES"); do
  python3.13 main.py \
    --mode sensor \
    --host localhost \
    --messages-per-second "$RATE" &
done

wait
