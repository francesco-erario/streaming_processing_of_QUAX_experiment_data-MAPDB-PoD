import csv
import os
import sys
import time

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
STREAM_TOPIC = os.environ.get("STREAM_TOPIC", "stream_topic")
POLL_INTERVAL_S = 2

output_path = sys.argv[1]
group_id = None
if len(sys.argv) > 2:
    group_id = sys.argv[2]

consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
partitions = [TopicPartition(STREAM_TOPIC, p) for p in consumer.partitions_for_topic(STREAM_TOPIC)]
consumer.assign(partitions)

admin = None
if group_id:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

f = open(output_path, "w", newline="")
writer = csv.writer(f)
writer.writerow(["timestamp", "partition", "end_offset", "committed_offset", "lag"])

while True:
    end_offsets = consumer.end_offsets(partitions)

    committed = {}
    if admin:
        #committed = admin.list_consumer_group_offsets(group_id)
        committed = admin.list_group_offsets({group_id: None})[group_id]
    for tp in partitions:
        end_offset = end_offsets[tp]
        if tp in committed:
            committed_offset = committed[tp].offset
            lag = end_offset - committed_offset
        else:
            committed_offset = None
            lag = None
        writer.writerow([time.time(), tp.partition, end_offset, committed_offset, lag])

    f.flush()
    time.sleep(POLL_INTERVAL_S)
