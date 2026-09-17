#!/bin/bash

# bash start_dask_cluster.sh n
# to start n < 4 worker

SCHEDULER_IP="10.67.22.68"
ALL_WORKERS=("10.67.22.154" "10.67.22.91" "10.67.22.15" "10.67.22.43")
DASK_BIN="/home/cluster/pyvenv/bin/dask"

N_WORKERS=${1:-${#ALL_WORKERS[@]}}
WORKERS=("${ALL_WORKERS[@]:0:$N_WORKERS}")

echo "Starting scheduler..."
ssh -n -f -i ~/.ssh/dask_cluster_key ubuntu@"$SCHEDULER_IP" \
    "nohup $DASK_BIN scheduler --port 8786 --dashboard-address :8797 > /home/ubuntu/dask.log 2>&1 < /dev/null &"

sleep 5

for w in "${WORKERS[@]}"; do
    echo "Starting worker at $w..."
    ssh -n -f -i ~/.ssh/dask_cluster_key ubuntu@"$w" \
        "nohup $DASK_BIN worker tcp://$SCHEDULER_IP:8786 > /home/ubuntu/dask.log 2>&1 < /dev/null &"
done
echo "Done. Dashboard: http://$SCHEDULER_IP:8797"
echo "SCHEDULER_ADDRESS=$SCHEDULER_IP:8786"