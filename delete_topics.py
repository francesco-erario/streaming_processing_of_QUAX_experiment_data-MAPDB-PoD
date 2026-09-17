import argparse
import os
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
STREAM_TOPIC = os.environ.get("STREAM_TOPIC", "stream_topic")
RESULTS_TOPIC = os.environ.get("RESULTS_TOPIC", "results_topic")


def parse_args():
    parser = argparse.ArgumentParser(description="Delete kafka Topics created in the pipeline.")
    parser.add_argument("--topics", nargs="+", default=[STREAM_TOPIC, RESULTS_TOPIC],
                         help="Topics to be deleted (default: stream_topic and results_topic)")
    return parser.parse_args()


def main():
    args = parse_args()

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    for topic in args.topics:
        try:
            admin.delete_topics([topic])
            print(f"Topic '{topic}' deleted.")
        except UnknownTopicOrPartitionError:
            print(f"Topic '{topic}' does not exist, skipped.")
    admin.close()


if __name__ == "__main__":
    main()