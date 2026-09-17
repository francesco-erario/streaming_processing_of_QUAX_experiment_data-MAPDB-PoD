#!/bin/bash

# example
# bash run_pipeline.sh <run_name> --dask-mode <cluster> --n-workers <2> --rate <2.0> --n-consumers <2>

RUN_NAME=$1
shift

DASK_MODE=single
N_WORKERS=1
RATE=4.2
MAX_IN_FLIGHT=2
KAFKA_PARTITIONS=$MAX_IN_FLIGHT
N_SLICES=4
N_CONSUMERS=1
DASHBOARD=0
GROUP_ID=bridge_group

# flags to vary run parapmeters
while [ $# -gt 0 ]; do
    case $1 in
        --dask-mode) DASK_MODE=$2; shift 2 ;;
        --n-workers) N_WORKERS=$2; shift 2 ;;
        --kafka-partitions) KAFKA_PARTITIONS=$2; shift 2 ;;
        --rate) RATE=$2; shift 2 ;;
        --max-in-flight) MAX_IN_FLIGHT=$2; shift 2 ;;
        --n-slices) N_SLICES=$2; shift 2 ;;
        --n-consumers) N_CONSUMERS=$2; shift 2 ;;
        --dashboard) DASHBOARD=1; shift ;;
        *) shift ;;
    esac
done

# paths for logging runs
RUN_DIR="runs/$RUN_NAME"
mkdir -p "$RUN_DIR/logs"

# start kafka broker cluster (clean restart)
bash stop_kafka_cluster.sh
sleep 2
bash start_kafka_cluster.sh
sleep 30 # give the KRaft quorum time to form before hitting the brokers with AdminClient
echo "Waiting for quorum cration."

# be sure topics are clear (recreate at every run)
python3 delete_topics.py
echo "Creating topics..."
sleep 2
python3 create_topics.py --partitions "$KAFKA_PARTITIONS"

echo "Starting Dask cluster..."
# start decided dask cluster
if [ "$DASK_MODE" = "single" ]; then
    bash stop_dask_single.sh
    sleep 2
    bash start_dask_single.sh "$N_WORKERS" > "$RUN_DIR/logs/dask_start.log"
else
    bash stop_dask_cluster.sh
    sleep 2
    bash start_dask_cluster.sh "$N_WORKERS" > "$RUN_DIR/logs/dask_start.log"
fi

SCHEDULER_ADDRESS=$(grep SCHEDULER_ADDRESS= "$RUN_DIR/logs/dask_start.log" | cut -d= -f2)

# start monitoring lags
python3 monitor_lag.py "$RUN_DIR/lag.csv" "$GROUP_ID" &
MONITOR_PID=$!
# monitor resource usage
python3 monitor_resources.py "$RUN_DIR/resources.csv" "$SCHEDULER_ADDRESS" &
RESOURCES_PID=$!

# run dashboard if flag is on
if [ "$DASHBOARD" -eq 1 ]; then
    streamlit run consumer_dashboard.py --server.port 8501 &
    DASHBOARD_PID=$!
fi

# start instances of chosen consumer (more than one if group)
BRIDGE_PIDS=""
i=1
while [ "$i" -le "$N_CONSUMERS" ]; do
    python3 stream_consumer-result_producer.py \
        --dask-client "$SCHEDULER_ADDRESS" \
        --run-id "$RUN_NAME" \
        --group-id "$GROUP_ID" \
        --benchmark-file "$RUN_DIR/consumer_benchmarks_$i.csv" \
        > "$RUN_DIR/logs/bridge_$i.log" &
    
    BRIDGE_PIDS="$BRIDGE_PIDS $!"
    i=$((i + 1))
    sleep 1
done

# start stream
RUN_ID="$RUN_NAME" python3 stream_producer.py \
    --rate "$RATE" \
    --max-in-flight "$MAX_IN_FLIGHT" \
    --n-slices "$N_SLICES" \
    --benchmark-file "$RUN_DIR/stream_benchmarks.csv"

wait $BRIDGE_PIDS

# kill lag monitoring
kill $MONITOR_PID $RESOURCES_PID $DASHBOARD_PID

# start decided dask cluster
if [ "$DASK_MODE" = "single" ]; then
    bash stop_dask_single.sh
else
    bash stop_dask_cluster.sh
fi

python3 delete_topics.py

bash stop_kafka_cluster.sh

echo "Run $RUN_NAME done, results in $RUN_DIR"
