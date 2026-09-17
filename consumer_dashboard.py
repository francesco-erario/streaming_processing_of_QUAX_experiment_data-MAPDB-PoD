import os
import json
import threading
import time

import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
RESULTS_TOPIC = os.environ.get("RESULTS_TOPIC", "results_topic")

FFT_SIZE = 2048
SAMPLE_RATE_HZ = 2e6 

FREQ_AXIS_MHZ = np.fft.fftshift(np.fft.fftfreq(FFT_SIZE, d=1 / SAMPLE_RATE_HZ)) / 1e6

REFRESH_SECONDS = 2  
MASK_HALF_WIDTH_BINS = 20  



class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest_batch_id = None
        self.latest_timestamp = None
        self.latest_mean = None      
        self.latest_std = None       
        self.cumulative_mean = None  
        self.n_batches = 0

    def update(self, batch_id, timestamp, mean, std):
        with self.lock:
            self.latest_batch_id = batch_id
            self.latest_timestamp = timestamp
            self.latest_mean = mean
            self.latest_std = std

            if self.cumulative_mean is None:
                self.cumulative_mean = mean.copy()
                self.n_batches = 1
            else:
                self.n_batches += 1
                self.cumulative_mean += (mean - self.cumulative_mean) / self.n_batches

    def snapshot(self):
        with self.lock:
            return (
                self.latest_batch_id,
                self.latest_timestamp,
                None if self.latest_mean is None else self.latest_mean.copy(),
                None if self.latest_std is None else self.latest_std.copy(),
                None if self.cumulative_mean is None else self.cumulative_mean.copy(),
                self.n_batches,
            )



def consume_results(state: SharedState):
    consumer = KafkaConsumer(
        RESULTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for message in consumer:
        msg = message.value
        batch_id = msg["batch_id"]
        timestamp = msg["timestamp"]
        mean = np.fft.fftshift(np.asarray(msg["mean"], dtype=np.float64))
        std = np.fft.fftshift(np.asarray(msg["std"], dtype=np.float64))
        state.update(batch_id, timestamp, mean, std)


@st.cache_resource
def get_state_and_start_thread():
    state = SharedState()
    thread = threading.Thread(target=consume_results, args=(state,), daemon=True)
    thread.start()
    return state



def plot_latest_batch(freq_mhz, mean, std, batch_id):
    fig, ax = plt.subplots(figsize=(6, 4))

    ax.plot(freq_mhz, mean, color="tab:blue", linewidth=1, label="Mean power")
    ax.fill_between(
        freq_mhz, mean - std, mean + std,
        color="tab:blue", alpha=0.25, label="±1 standard deviation",
    )

    ax.set_xlabel("Frequency relative to ν_LO (MHz)")
    ax.set_ylabel("Power (arbitrary units, |FFT|²)")
    ax.set_title(f"Batch {batch_id} spectrum")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_latest_batch_masked(freq_mhz, mean, std, batch_id, half_width=MASK_HALF_WIDTH_BINS):
    center_idx = int(np.argmin(np.abs(freq_mhz)))
    lo = max(center_idx - half_width, 0)
    hi = min(center_idx + half_width + 1, len(freq_mhz))

    masked_mean = mean.copy()
    masked_std = std.copy()
    masked_mean[lo:hi] = np.nan
    masked_std[lo:hi] = np.nan

    fig, ax = plt.subplots(figsize=(6, 4))

    ax.plot(freq_mhz, masked_mean, color="tab:green", linewidth=1, label="Mean power")
    ax.fill_between(
        freq_mhz, masked_mean - masked_std, masked_mean + masked_std,
        color="tab:green", alpha=0.25, label="±1 standard deviation",
    )

    ax.set_xlabel("Frequency relative to ν_LO (MHz)")
    ax.set_ylabel("Power (arbitrary units, |FFT|²)")
    ax.set_title(f"Batch {batch_id} spectrum (Δν≈0 bin masked)")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_cumulative(freq_mhz, cumulative, n_batches):
    """Cumulative spectrum: average over all batches received so far."""
    fig, ax = plt.subplots(figsize=(6, 4))

    ax.plot(freq_mhz, cumulative, color="tab:orange", linewidth=1)

    ax.set_xlabel("Frequency relative to ν_LO (MHz)")
    ax.set_ylabel("Cumulative mean power (arbitrary units, |FFT|²)")
    ax.set_title(f"Cumulative spectrum over {n_batches} batches")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig



st.set_page_config(page_title="QUAX live monitor", layout="wide")
st.title("QUAX — Live spectrum monitor")

state = get_state_and_start_thread()

status_placeholder = st.empty()
col1, col2, col3 = st.columns(3)
col1.subheader("Latest batch spectrum")
col2.subheader("Cumulative spectrum (average over all batches)")
col3.subheader("Latest batch spectrum (Δν≈0 bin masked)")
latest_placeholder = col1.empty()
cumulative_placeholder = col2.empty()
masked_placeholder = col3.empty()

last_rendered_batch_id = None

while True:
    batch_id, timestamp, mean, std, cumulative, n_batches = state.snapshot()

    if mean is None:
        status_placeholder.info("Waiting for the first batch from Kafka...")
    elif batch_id != last_rendered_batch_id:
        status_placeholder.success(
            f"Latest batch: **{batch_id}** — timestamp: {timestamp} — "
            f"batches received so far: {n_batches}"
        )

        fig_latest = plot_latest_batch(FREQ_AXIS_MHZ, mean, std, batch_id)
        latest_placeholder.pyplot(fig_latest, clear_figure=True)

        fig_cumulative = plot_cumulative(FREQ_AXIS_MHZ, cumulative, n_batches)
        cumulative_placeholder.pyplot(fig_cumulative, clear_figure=True)

        fig_masked = plot_latest_batch_masked(FREQ_AXIS_MHZ, mean, std, batch_id)
        masked_placeholder.pyplot(fig_masked, clear_figure=True)

        last_rendered_batch_id = batch_id

    time.sleep(REFRESH_SECONDS)