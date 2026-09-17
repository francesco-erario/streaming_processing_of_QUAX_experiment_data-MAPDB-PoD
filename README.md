# MAPD Project 6 Group 06

Everything runs natively on CloudVeneto VMs: three Kafka brokers (KRaft), one Dask scheduler and up to four worker VMs, plus a single-node setup.

## Repository layout

```         
stream_producer.py                  reads batches from S3, slices them, publishes to stream_topic
stream_consumer-result_producer.py  consumes batches, submits FFT tasks to Dask, publishes to results_topic
consumer_dashboard.py               Streamlit dashboard on the results topic
bench.py                            small timing recorder used by producer and consumer
create_topics.py / delete_topics.py topic management
monitor_lag.py / monitor_resources.py   background monitors (consumer lag, CPU/RAM of client and workers)
start_/stop_kafka_cluster.sh        Kafka brokers over ssh
start_/stop_dask_cluster.sh         distributed Dask (scheduler + N worker VMs)
start_/stop_dask_single.sh          single-node Dask (N workers on one VM)
run_pipeline.sh                     driver that starts everything, runs one configuration, tears it down
plots.py / benchmark_plots.py       plotting of the online runs
runs/                               one folder per online run (CSV benchmarks, lag, resources, logs, command.txt)
plots_out/                          plots produced from runs/
benchmark_cluster_offline/          offline Dask scaling benchmark on the cluster (+ its plots)
benchmark_single_node_offline/      same benchmark on a single node
```

## Configuration

Credentials and endpoints are read from a `.env` file (not committed). Python dependencies are pinned in `requirements.txt`.

## How to run

A single run is launched with

``` bash
bash run_pipeline.sh <run_name> [--dask-mode single|cluster] [--n-workers N] \
     [--n-slices N] [--rate S] [--max-in-flight N] [--kafka-partitions N] \
     [--n-consumers N] [--dashboard]
```

The results in `runs/` were produced in this way: a baseline on the single node and one on the cluster, then one run per parameter varied around them (number of workers, slices, producer rate, in-flight batches, number of consumers), plus a stress test and a run with the configuration that performed best. The plots are then generated from those files.

The two `benchmark_*_offline/` folders contain a separate, Kafka-free measurement: the same FFT workload is submitted directly to Dask on a synthetic batch, sweeping the number of workers and slices with several repetitions, to measure scaling and the warm-up behaviour independently of the streaming layer. Each folder holds the benchmark script, the resulting CSV and the scripts that produce its plots.
