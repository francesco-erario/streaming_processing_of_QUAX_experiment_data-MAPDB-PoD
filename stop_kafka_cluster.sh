#!/bin/bash

BROKERS=("10.67.22.134" "10.67.22.5" "10.67.22.97")

for broker in "${BROKERS[@]}"; do
    echo "Stopping broker at $broker..."
    ssh -n -i ~/.ssh/kafka_cluster_key ubuntu@"$broker" "pkill -f kafka.Kafka" &
done

echo "Waiting a few seconds for shutdown..."
sleep 5

echo "Now checking if processes have stopped properly..."
for ip in "${BROKERS[@]}"; do
    echo -n "$ip: "
    ssh -i ~/.ssh/kafka_cluster_key ubuntu@"$ip" "pgrep -f kafka.Kafka || echo Stopped"
done

#echo 'If processes still running use: ssh ubuntu@<broker-ip> "pkill -f kafka.Kafka"'