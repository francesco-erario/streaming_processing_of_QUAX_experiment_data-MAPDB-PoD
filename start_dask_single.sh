#!/bin/bash

HOST_IP="10.67.22.233"
DASK_BIN="/home/cluster/pyvenv/bin/dask"
N_WORKERS=${1:-1}

echo "Starting scheduler..."
ssh -n -f -i ~/.ssh/dask_cluster_key ubuntu@"$HOST_IP" \
    "nohup $DASK_BIN scheduler --port 8786 --dashboard-address :8797 > /home/ubuntu/dask.log 2>&1 < /dev/null &"

sleep 5

for i in $(seq 1 "$N_WORKERS"); do
    echo "Starting worker $i at $HOST_IP..."
    ssh -n -f -i ~/.ssh/dask_cluster_key ubuntu@"$HOST_IP" \
        "nohup $DASK_BIN worker tcp://$HOST_IP:8786 > /home/ubuntu/dask_worker_$i.log 2>&1 < /dev/null &"
done

echo "Done. Dashboard: http://$HOST_IP:8797"
echo "SCHEDULER_ADDRESS=$HOST_IP:8786"