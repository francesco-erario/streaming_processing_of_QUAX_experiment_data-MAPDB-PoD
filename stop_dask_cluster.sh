#!/bin/bash

SCHEDULER_IP="10.67.22.68"
WORKERS=("10.67.22.154" "10.67.22.91" "10.67.22.15" "10.67.22.43")

for w in "${WORKERS[@]}"; do
    ssh -n -i ~/.ssh/dask_cluster_key ubuntu@"$w" "pkill -f 'dask worker'" &
done
wait

ssh -n -i ~/.ssh/dask_cluster_key ubuntu@"$SCHEDULER_IP" "pkill -f 'dask scheduler'"