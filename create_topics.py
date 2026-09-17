import argparse
import os
import time
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
STREAM_TOPIC = os.environ.get("STREAM_TOPIC", "stream_topic")
RESULTS_TOPIC = os.environ.get("RESULTS_TOPIC", "results_topic")


def parse_args():
    parser = argparse.ArgumentParser(description="Create Kafka Topics for the pipeline.")
    parser.add_argument("--partitions", type=int, default=4,
                         help="Partition number")
    parser.add_argument("--replication-factor", type=int, default=3)
    return parser.parse_args()


def create_topic(admin, name, partitions, replication_factor):
    topic = NewTopic(name=name, num_partitions=partitions, replication_factor=replication_factor)
    try:
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{name}' created (partitions={partitions}, replication_factor={replication_factor})")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists, skip creation.")


def main():
    args = parse_args()
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    create_topic(admin, STREAM_TOPIC, args.partitions, args.replication_factor)
    create_topic(admin, RESULTS_TOPIC, 1, args.replication_factor)

    time.sleep(2)
    print("\nTopics on cluster:")
    print(admin.list_topics())

    admin.close()


if __name__ == "__main__":
    main()