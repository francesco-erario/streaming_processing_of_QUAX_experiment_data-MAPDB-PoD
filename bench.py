import contextlib
import csv
import os
import threading
import time

class BenchmarkRecorder:
    def __init__(self, path: str, config: dict | None = None):
        self.path = path
        self.config = config or {}
        self._config_keys = list(self.config.keys())
        self._lock = threading.Lock()

        header = ["timestamp", "batch_id", "stage", "duration_s"] + self._config_keys

        if not os.path.exists(self.path):
            with open(self.path, "w", newline="") as f:
                csv.writer(f).writerow(header)
        else:
            with open(self.path, "r", newline="") as f:
                existing_header = next(csv.reader(f), [])

    def record(self, batch_id: str, stage: str, duration: float) -> None:
        row = [time.time(), batch_id, stage, f"{duration:.6f}"] + [self.config.get(k) for k in self._config_keys]
        with self._lock:
            with open(self.path, "a", newline="") as f:
                csv.writer(f).writerow(row)

    @contextlib.contextmanager
    def timed(self, batch_id: str, stage: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.record(batch_id, stage, time.perf_counter()-t0)
