import argparse
import io
import json
import logging
import os
import re
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from botocore.config import Config as BotocoreConfig
from dotenv import load_dotenv
from kafka import KafkaProducer
from bench import BenchmarkRecorder
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
S3_BUCKET = os.environ.get("S3_BUCKET", "quax")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")

STREAM_TOPIC = os.environ.get("STREAM_TOPIC", "stream_topic")

# Data format (confermato via buck_test.py, il PDF era 8193x1024 a typo)
ROWS, COLS = 8192, 1024
EXPECTED_BYTES = ROWS * COLS * 4
FILENAME_RE = re.compile(r"duck_([iq])_(\d{5})\.dat$")

N_SLICES = 4
RATE_SECONDS = 4.2
MAX_IN_FLIGHT = 2
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.5
BENCHMARK_FILE = "benchmarks_r1.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
)
log = logging.getLogger("r1_producer")
logging.getLogger("kafka").setLevel(logging.WARNING)

def permanent_s3_error(exc: Exception) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        return code in ("NoSuchKey", "404", "NoSuchBucket")
    if isinstance(exc, ValueError):
        return True
    return False


def retry_with_backoff(func, *, max_retries: int, description: str, is_permanent=lambda e: False):
    attempt = 0
    while True:
        try:
            return func()
        except Exception as exc:
            if is_permanent(exc):
                log.error("%s: permanent error, not retrying %s", description, exc)
                raise
            attempt += 1
            if attempt > max_retries:
                log.error("%s: giving up after %d attempt(s) %s", description, attempt, exc)
                raise
            delay = RETRY_BACKOFF_BASE ** attempt
            log.warning("%s: attempt %d/%d failed (%s), retrying in %.1fs", description, attempt, max_retries, exc, delay)
            time.sleep(delay)

# S3 client, download

def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        verify=False,
        config=BotocoreConfig(max_pool_connections=64),
    )


def discover_pairs(s3_client) -> list[str]:
    seen = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            m = FILENAME_RE.search(obj["Key"])
            if not m:
                continue
            component, batch_id = m.group(1), m.group(2)
            seen.setdefault(batch_id, set()).add(component)

    complete = sorted(bid for bid, comps in seen.items() if {"i", "q"} <= comps)
    log.info("Discovered %d complete pairs in bucket '%s'", len(complete), S3_BUCKET)
    return complete


transfer_config = TransferConfig(max_concurrency=16, use_threads=True)


def download_component_once(s3_client, component: str, batch_id: str) -> bytes:
    key = f"duck_{component}_{batch_id}.dat"
    buf = io.BytesIO()
    s3_client.download_fileobj(S3_BUCKET, key, buf, Config = transfer_config)
    raw = buf.getvalue()
    if len(raw) != EXPECTED_BYTES:
        raise ValueError(f"{key}: expected {EXPECTED_BYTES} bytes, got {len(raw)}")
    return raw


def download_component(s3_client, component: str, batch_id: str, bench: BenchmarkRecorder, max_retries: int) -> bytes:
    with bench.timed(batch_id, f"download_{component}"):
        return retry_with_backoff(
            lambda: download_component_once(s3_client, component, batch_id),
            max_retries = max_retries,
            description = f"download {component} (batch {batch_id})",
            is_permanent = permanent_s3_error,
        )


def download_pair(s3_client, batch_id: str, bench: BenchmarkRecorder, max_retries: int) -> tuple[bytes, bytes]:
    with bench.timed(batch_id, "download_total"):
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_i = pool.submit(download_component, s3_client, "i", batch_id, bench, max_retries)
            fut_q = pool.submit(download_component, s3_client, "q", batch_id, bench, max_retries)
            data_i = fut_i.result()
            data_q = fut_q.result()
    return data_i, data_q


# Kafka publishing

def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        max_request_size=100 * 1024 * 1024,
        acks = "all"
    )

def build_message(batch_id: str, slice_idx: int, n_slices: int, slice_i: bytes, slice_q: bytes, batch_timestamp: float) -> bytes:
    
    n_bytes_slice = len(slice_i)   # uguale per slice_q per costruzione
    header = {
        "batch_id":   batch_id,
        "slice_idx":  slice_idx,
        "n_slices":   n_slices,
        "n_bytes_i":  n_bytes_slice,
        "n_bytes_q":  n_bytes_slice,
        "dtype":      "float32-le",
        "timestamp":  batch_timestamp,
    }
    header_bytes = json.dumps(header).encode("utf-8")
    payload = slice_i + slice_q   
    return struct.pack(">I", len(header_bytes)) + header_bytes + payload


# Producer loop

def process_batch(s3_client, producer: KafkaProducer, batch_id: str, bench: BenchmarkRecorder, n_slices: int, max_retries: int) -> None:
    with bench.timed(batch_id, "batch_total"):
        raw_i, raw_q = download_pair(s3_client, batch_id, bench, max_retries)
        batch_timestamp = time.time()
        
        with bench.timed(batch_id, "slicing"):
            slice_size = EXPECTED_BYTES // n_slices
            slices_i = [raw_i[k * slice_size:(k + 1) * slice_size] for k in range(n_slices)]
            slices_q = [raw_q[k * slice_size:(k + 1) * slice_size] for k in range(n_slices)]
            

        with bench.timed(batch_id, "publish"):
            messages = [build_message(batch_id, idx, n_slices, si, sq, batch_timestamp) for idx, (si, sq) in enumerate(zip(slices_i, slices_q))]
            futures = [producer.send(STREAM_TOPIC, key=batch_id.encode(), value=msg) for msg in messages]
            for future in futures:
                future.get(timeout=30)

    log.info("batch %s published (%d slices)", batch_id, n_slices)


def run_producer(batch_ids: list[str], bench: BenchmarkRecorder, rate_seconds: float, max_in_flight: int, n_slices: int, max_retries: int) -> None:

    s3_client = make_s3_client()
    producer = make_producer()
    num_partitions = len(producer.partitions_for(STREAM_TOPIC) or [])
    if max_in_flight < num_partitions:
        log.warning("max_in_flight=%d but topic %s has %d partitions: only %d of them can be fed at a time", max_in_flight, STREAM_TOPIC, num_partitions, max_in_flight)
    in_flight = threading.Semaphore(max_in_flight)

    def worker(batch_id: str):
        try:
            process_batch(s3_client, producer, batch_id, bench, n_slices, max_retries)
        except Exception:
            log.exception("batch %s failed permanently, skipping", batch_id)
        finally:
            in_flight.release()

    threads: list[threading.Thread] = []
    next_tick = time.monotonic()

    try:
        for batch_id in batch_ids:
            in_flight.acquire()
            now = time.monotonic()
            if now < next_tick:
                time.sleep(next_tick - now)
            next_tick += rate_seconds

            t = threading.Thread(target=worker, args=(batch_id,), name=f"batch-{batch_id}")
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed.")


# CLI / entrypoint

def parse_args():
    parser = argparse.ArgumentParser(description="R1 producer for the QUAX streaming pipeline")
    parser.add_argument("--rate", type=float, default=None)
    parser.add_argument("--max-in-flight", type=int, default=None)
    parser.add_argument("--n-slices", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--benchmark-file", type=str, default=None)
    return parser.parse_args()


def resolve(cli_value, env_var, default, cast=float):
    if cli_value is not None:
        return cli_value
    return cast(os.environ.get(env_var, default))


def main():
    args = parse_args()

    rate_seconds = resolve(args.rate, "RATE_SECONDS", RATE_SECONDS, float)
    max_in_flight = resolve(args.max_in_flight, "MAX_IN_FLIGHT", MAX_IN_FLIGHT, int)
    n_slices = resolve(args.n_slices, "N_SLICES", N_SLICES, int)
    max_retries = resolve(args.max_retries, "MAX_RETRIES", MAX_RETRIES, int)
    benchmark_file = args.benchmark_file or os.environ.get("R1_BENCHMARK_FILE", BENCHMARK_FILE)
    run_id = os.environ.get("RUN_ID") or time.strftime("%Y%m%d_%H%M%S")

    log.info("Config: rate=%.2fs max_in_flight=%d n_slices=%d max_retries=%d benchmark_file=%s", rate_seconds, max_in_flight, n_slices, max_retries, benchmark_file)

    bench = BenchmarkRecorder(benchmark_file,
		config={
            		"n_slices": n_slices,
            		"run_id": run_id,
        		},
    		)

    s3_client = make_s3_client()
    batch_ids = discover_pairs(s3_client)
    if not batch_ids:
        log.error("No complete duck_i/duck_q pairs found in bucket '%s'", S3_BUCKET)
        sys.exit(1)

    run_producer(batch_ids, bench, rate_seconds, max_in_flight, n_slices, max_retries)


if __name__ == "__main__":
    main()