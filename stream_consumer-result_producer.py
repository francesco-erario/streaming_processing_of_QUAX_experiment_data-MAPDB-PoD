from kafka import KafkaConsumer, KafkaProducer
from dask.distributed import Client, as_completed
from dotenv import load_dotenv
from bench import BenchmarkRecorder

import argparse
import numpy as np
import os
import struct
import json
import threading
import time

# ----- ENV VARIABLES -----

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
STREAM_TOPIC = os.environ.get("STREAM_TOPIC", "stream_topic")
RESULTS_TOPIC = os.environ.get("RESULTS_TOPIC", "results_topic")
DASK_CLIENT = os.environ.get("DASK_CLIENT")
MAX_BYTES = int(os.environ.get("MAX_BYTES"))

# decides the hierarchy of the configs
def resolve(cli_value, env_var, default=None, cast=str):
    if cli_value is not None:
        return cli_value
    raw = os.environ.get(env_var, default)
    return cast(raw) if raw is not None else None

# ----- ----- -----

# ----- FUNCTIONS -----

# function to test dask connection
# def test_function(batch_id, data_i, data_q):
#     return f"Batch {batch_id}: received data_i.shape={data_i.shape}, data_q.shape={data_q.shape}"

FFT_SIZE = 2048

# unpack the raw message and compute fft for chunks of data
def compute_fft_chunk(raw, header_len, n_bytes_i, n_bytes_q):
    # extract payload
    payload = memoryview(raw)[4 + header_len:]
    # extract slice i
    # data are float32 little-endian -> convert using dtype="<f4"
    # frombuffer gives a 1D flat array
    chunk_i = np.frombuffer(payload[:n_bytes_i], dtype="<f4")
    # extract slice q
    chunk_q = np.frombuffer(payload[n_bytes_i:n_bytes_i + n_bytes_q], dtype="<f4")

    signal = chunk_i + 1j * chunk_q
    matrix = signal.reshape(-1, FFT_SIZE)
    n_scans = len(matrix)
    ps = np.abs(np.fft.fft(matrix, axis=1)) ** 2
    return ps.mean(axis=0), ps.std(axis=0), n_scans

# combine results from fft
def combine_chunks(batch_id, timestamp, results):
    total_scans = 0
    for r in results:
        total_scans += r[2]
    #mean 
    weighted_mean_sum = 0
    for r in results:
        weighted_mean_sum += r[2] * r[0]
    combined_mean = weighted_mean_sum / total_scans
    #comb var 
    weighted_var_sum = 0
    for r in results:
        weighted_var_sum += r[2] * (r[1] ** 2 + (r[0] - combined_mean) ** 2)
    combined_std = np.sqrt(weighted_var_sum / total_scans)

    return batch_id, timestamp, combined_mean, combined_std

# function to publish results to result_topic
def publish_result(results_producer, bench, batch_id, original_timestamp, spectrum, spectrum_std):
    msg = {
        "batch_id": batch_id,
        "timestamp": original_timestamp,
        "mean": spectrum.tolist(),
        "std": spectrum_std.tolist()
    }
    encoded_msg = json.dumps(msg).encode("utf-8")
    results_producer.send(RESULTS_TOPIC, key=batch_id.encode(), value=encoded_msg)
    bench.record(batch_id, "end_to_end_latency", time.time() - original_timestamp)

# function to handle results without synchronous calls
def result_handler(ac, results_producer, bench, submit_times):
    # actually a blocking cycle but uses a different thread
    # blocks when whatever future added to ac is ready, independetly on the order
    # using while loop always True to prevent that the "StopIteration" triggering blocks the loop
    while True:
        try:
            # check if a future is ready otherwise "StopIteration"
            # using directly result = future.result() in a for loop would trigger "StopIteration" and end loop
            future = next(ac)
        except StopIteration:
            # wait for a brief moment if future is not ready
            time.sleep(0.02)
            continue

        # when future is retrieved the program can publish
        try:
            result = future.result()
        except Exception as e:
            print(f"[dask ERROR] {e}", flush=True)
            submit_times.pop(future, None)
            continue
        batch_id, timestamp, spectrum, spectrum_std = result
        # take the times of the previous part
        t_submit = submit_times[future]
        # record entire dask time (after submission)
        bench.record(batch_id, "dask_compute", time.perf_counter() - t_submit)
        publish_result(results_producer, bench, batch_id, timestamp, spectrum, spectrum_std)
        # take out of the dict
        submit_times.pop(future)
        print(f"Published result for batch {batch_id}", flush=True)

# ----- ----- -----

def parse_args():
    parser = argparse.ArgumentParser(description="R3 bridge consumer")
    parser.add_argument("--benchmark-file", type=str, default="benchmarks_stream_consumer.csv")
    parser.add_argument("--dask-client", type=str, default=None)
    parser.add_argument("--group-id", type=str, default=None)
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--consumer-timeout-ms", type=int, default=30000) # timer when consumer does not receive messages
    return parser.parse_args()


def main():

    args = parse_args()
    # if values are passed through flegs use those, otherwise environment values or default 
    dask_client_addr = resolve(args.dask_client, "DASK_CLIENT", DASK_CLIENT, str)
    run_id = args.run_id or os.environ.get("RUN_ID") or time.strftime("%Y%m%d_%H%M%S")

    # object to write benchmarks on .csv file
    # config dict adds configs of the specific run as columns inside the file
    bench = BenchmarkRecorder(args.benchmark_file, config={
        "run_id": run_id,
        "group_id": args.group_id,
        "dask_client": dask_client_addr,
    })

    # define dask client (scheduler address)
    client = Client(dask_client_addr)
    print(client)

    # create the data stream consumer
    consumer = KafkaConsumer(
        STREAM_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=args.group_id, # becomes a consumer group if not None
        auto_offset_reset="earliest", # starts reading from beginning
        max_partition_fetch_bytes=MAX_BYTES,
        fetch_max_bytes=MAX_BYTES,
        consumer_timeout_ms=args.consumer_timeout_ms, # timer when consumer does not receive messages
    )
    while not consumer.assignment():
        consumer.poll(timeout_ms=200)

    results_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    # object that accumulates work in progress (asynchronous work)
    ac = as_completed()
    submit_times = {}
    # start second thread to read and publish results from dask
    # (main thread handles the loop -> message reading and sending)
    threading.Thread(target=result_handler, args=(ac, results_producer, bench, submit_times), daemon=True).start()


    # create a empty dictionary to hold messages
    # if messages do not come ordered by batches
    pending = {}
    # organization of the dictionary:
    # key = batch_id
    # value = future containg results from fft computing

    for message in consumer:
        print(f"Got offset={message.offset} size={len(message.value)}", flush=True)
        raw = message.value # select raw bytes

        # extract header
        # take first 4 bytes that contain the header length
        # unpack them as 32 bit numbers in big-endian (">I")
        header_len = struct.unpack(">I", raw[:4])[0] 
        header_bytes = raw[4:4 + header_len] # extract header JSON
        header = json.loads(header_bytes) # translate into python dictionary

        # extract other info from header
        batch_id = header["batch_id"]
        slice_idx = header["slice_idx"]
        n_slices = header["n_slices"]
        timestamp = header["timestamp"]

        # create new entry if does not correspond to any batch_id already stored
        if batch_id not in pending:
            pending[batch_id] = []

        with bench.timed(batch_id, "dask_submit"):
            # client.scatter() load data directly on a worker and returns a object that points to where data now live
            # it is faster w.r.t. passing directly the data when they are large (16MB in this case)
            raw_future = client.scatter(raw)
            chunk_future = client.submit(compute_fft_chunk, raw_future, header_len, header["n_bytes_i"], header["n_bytes_q"])

        # store current value with right index
        pending[batch_id].append(chunk_future)

        # TEST: check slices gathering
        # print(f"batch {batch_id}: gathered {len(pending[batch_id])}/{n_slices} slice")

        # extract values from dictionary and assemble data
        # delete batch_id entry once data have been extracted
        if len(pending[batch_id]) == n_slices:
            chunk_futures = pending.pop(batch_id)
            # submit to combine function
            future = client.submit(combine_chunks, batch_id, timestamp, chunk_futures)
            submit_times[future] = time.perf_counter()
            ac.add(future)
            print(f"Batch {batch_id} submitted ({n_slices} chunks)", flush=True)


    deadline = time.time() + 120
    while submit_times and time.time() < deadline:
        time.sleep(0.05)

    results_producer.flush()
    results_producer.close()
    consumer.close()
    client.close()
    print("Bridge shut down.", flush=True)


if __name__ == "__main__":
    main()