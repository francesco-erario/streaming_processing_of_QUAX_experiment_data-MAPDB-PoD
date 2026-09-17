#!/bin/bash

HOST_IP="10.67.22.233"

ssh -n -i ~/.ssh/dask_cluster_key ubuntu@"$HOST_IP" "pkill -f 'dask worker'"
ssh -n -i ~/.ssh/dask_cluster_key ubuntu@"$HOST_IP" "pkill -f 'dask scheduler'"