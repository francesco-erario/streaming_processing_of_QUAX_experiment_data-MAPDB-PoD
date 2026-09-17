#!/bin/bash

BROKERS=("10.67.22.134" "10.67.22.5" "10.67.22.97")
KAFKA_HOME="/opt/kafka"

for broker in "${BROKERS[@]}"; do
    echo "Starting broker at $broker..."
    # -f syas to put ssh connection in background (loop goes to next borker activation without waiting)
    # with -n ssh does not read local stdin to avoid races between connections
    ssh -n -f -i ~/.ssh/kafka_cluster_key ubuntu@"$broker" \
        "cd $KAFKA_HOME && nohup bin/kafka-server-start.sh config/kraft/server.properties > kafka.log 2>&1 < /dev/null &"
done

echo "All 3 brokers started. Wait ~30s for quorum formation."
echo "Done. Check log with: ssh -i ~/.ssh/kafka_cluster_key ubuntu@<broker-ip> tail -f $KAFKA_HOME/kafka.log"