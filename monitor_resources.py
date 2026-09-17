# monitor CPU and RAM usage while a run is going on
# it checks the local machine (where this script runs) and every dask worker
# everything gets written to a csv file every few seconds

import sys
import csv
import time
import psutil
from dask.distributed import Client

csv_path = sys.argv[1]
dask_address = sys.argv[2]

INTERVAL = 2  # seconds between each measurement

# connect to the dask cluster that is already running
client = Client(dask_address)

# write the header before starting the loop
with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "location", "cpu_percent", "mem_percent"])

print("Resource monitor started, writing to", csv_path, flush=True)

# psutil.cpu_percent() needs a first "throwaway" call to have a baseline,
# otherwise the first real measurement would be meaningless
psutil.cpu_percent()

while True:
    now = time.time()

    local_cpu = psutil.cpu_percent()
    local_mem = psutil.virtual_memory().percent

    # client.run() executes the given function directly on every worker process and returns a dict
    # {worker_address: return_value}
    workers_cpu = client.run(psutil.cpu_percent)
    workers_mem = client.run(lambda: psutil.virtual_memory().percent)

    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([now, "local", local_cpu, local_mem])
        for worker_address in workers_cpu:
            writer.writerow([now, worker_address, workers_cpu[worker_address], workers_mem[worker_address]])

    time.sleep(INTERVAL)