import os
import sys
import glob
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def parse_command(path):
    f = open(path)
    line = f.read().strip()
    f.close()
    parts = line.split()
    args = {}
    i = 0
    while i < len(parts):
        p = parts[i]
        if p.startswith('--'):
            key = p[2:]
            if i + 1 < len(parts) and not parts[i + 1].startswith('--'):
                args[key] = parts[i + 1]
                i += 2
            else:
                args[key] = True
                i += 1
        else:
            i += 1
    return args


def find_runs(base_dir):
    runs = []
    for name in sorted(os.listdir(base_dir)):
        path = os.path.join(base_dir, name)
        if os.path.isdir(path) and os.path.exists(os.path.join(path, 'command.txt')):
            runs.append(path)
    return runs


def load_run(run_dir):
    label = os.path.basename(run_dir.rstrip('/'))
    cmd_path = os.path.join(run_dir, 'command.txt')
    args = parse_command(cmd_path) if os.path.exists(cmd_path) else {}

    stream_path = os.path.join(run_dir, 'stream_benchmarks.csv')
    if os.path.exists(stream_path):
        stream = pd.read_csv(stream_path)
    else:
        stream = pd.DataFrame()

    consumer_files = sorted(glob.glob(os.path.join(run_dir, 'consumer_benchmarks_*.csv')))
    consumer_frames = []
    for cf in consumer_files:
        df = pd.read_csv(cf)
        df['consumer_file'] = os.path.basename(cf)
        consumer_frames.append(df)
    if consumer_frames:
        consumer = pd.concat(consumer_frames, ignore_index=True)
    else:
        consumer = pd.DataFrame()

    lag_path = os.path.join(run_dir, 'lag.csv')
    if os.path.exists(lag_path):
        lag = pd.read_csv(lag_path)
    else:
        lag = pd.DataFrame()

    res_path = os.path.join(run_dir, 'resources.csv')
    if os.path.exists(res_path):
        resources = pd.read_csv(res_path)
    else:
        resources = pd.DataFrame()

    return {
        'dir': run_dir,
        'label': label,
        'args': args,
        'stream': stream,
        'consumer': consumer,
        'lag': lag,
        'resources': resources,
    }


def plot_stream_stages(runs, out_dir):
    stages = ['download_i', 'download_q', 'download_total', 'slicing', 'publish']
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    axes = axes.flatten()
    for idx, stage in enumerate(stages):
        ax = axes[idx]
        data = []
        labels = []
        for r in runs:
            df = r['stream']
            if df.empty:
                continue
            vals = df[df['stage'] == stage]['duration_s'].values
            if len(vals) > 0:
                data.append(vals)
                labels.append(r['label'])
        if data:
            ax.boxplot(data, labels=labels, showfliers=False)
        ax.set_title(stage)
        ax.set_ylabel('Duraation (s)')
        ax.tick_params(axis='x', rotation=45)
    axes[-1].axis('off')
    fig.suptitle('Producer (stream_benchmarks) - duration per stage')
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, 'stream_stages.png'), dpi=150)
    plt.close(fig)


def plot_consumer_stages(runs, out_dir):
    stages = ['dask_submit', 'dask_compute', 'end_to_end_latency']
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    for idx, stage in enumerate(stages):
        ax = axes[idx]
        data = []
        labels = []
        for r in runs:
            df = r['consumer']
            if df.empty:
                continue
            vals = df[df['stage'] == stage]['duration_s'].values
            if len(vals) > 0:
                data.append(vals)
                labels.append(r['label'])
        if data:
            ax.boxplot(data, labels=labels, showfliers=False)
        ax.set_title(stage)
        ax.set_ylabel('duration (s)')
        ax.tick_params(axis='x', rotation=45)
    fig.suptitle('Consumer (dask) - duration per stage')
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, 'consumer_stages.png'), dpi=150)
    plt.close(fig)


def plot_lag(runs, out_dir):
    fig, ax = plt.subplots(figsize=(10, 6))
    has_data = False
    for r in runs:
        df = r['lag']
        if df.empty:
            continue
        df = df.dropna(subset=['lag'])
        if df.empty:
            continue
        grouped = df.groupby('timestamp')['lag'].sum().reset_index()
        t0 = grouped['timestamp'].min()
        ax.plot(grouped['timestamp'] - t0, grouped['lag'], label=r['label'])
        has_data = True
    ax.set_xlabel('Time since the start of the run (s)')
    ax.set_ylabel('total lag (partitions sum)')
    ax.set_title('Lag Kafka in time')
    if has_data:
        ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, 'lag.png'), dpi=150)
    plt.close(fig)


def plot_resources(runs, out_dir):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    ax_cpu, ax_mem = axes
    for r in runs:
        df = r['resources']
        if df.empty:
            continue
        local = df[df['location'] == 'local']
        if local.empty:
            continue
        t0 = local['timestamp'].min()
        ax_cpu.plot(local['timestamp'] - t0, local['cpu_percent'], label=r['label'])
        ax_mem.plot(local['timestamp'] - t0, local['mem_percent'], label=r['label'])
    ax_cpu.set_title('CPU client')
    ax_cpu.set_xlabel('time (s)')
    ax_cpu.set_ylabel('cpu %')
    ax_cpu.legend()
    ax_mem.set_title('Memory client')
    ax_mem.set_xlabel('time (s)')
    ax_mem.set_ylabel('mem %')
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, 'resources_local.png'), dpi=150)
    plt.close(fig)

    fig2, axes2 = plt.subplots(1, 2, figsize=(14, 6))
    ax_cpu2, ax_mem2 = axes2
    for r in runs:
        df = r['resources']
        if df.empty:
            continue
        workers = df[df['location'] != 'local']
        if workers.empty:
            continue
        avg = workers.groupby('timestamp')[['cpu_percent', 'mem_percent']].mean().reset_index()
        t0 = avg['timestamp'].min()
        ax_cpu2.plot(avg['timestamp'] - t0, avg['cpu_percent'], label=r['label'])
        ax_mem2.plot(avg['timestamp'] - t0, avg['mem_percent'], label=r['label'])
    ax_cpu2.set_title('Average CPU worker Dask')
    ax_cpu2.set_xlabel('time (s)')
    ax_cpu2.set_ylabel('cpu %')
    ax_cpu2.legend()
    ax_mem2.set_title('Average memory worker Dask')
    ax_mem2.set_xlabel('time (s)')
    ax_mem2.set_ylabel('mem %')
    fig2.tight_layout()
    fig2.savefig(os.path.join(out_dir, 'resources_workers.png'), dpi=150)
    plt.close(fig2)


def plot_summary_bars(runs, out_dir):
    metrics = [
        ('stream', 'download_total', 'Total download (producer)'),
        ('stream', 'publish', 'Publish Kafka (producer)'),
        ('consumer', 'end_to_end_latency', 'Latency (consumer)'),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    for idx, (src, stage, title) in enumerate(metrics):
        ax = axes[idx]
        means = []
        stds = []
        labels = []
        for r in runs:
            df = r[src]
            if df.empty:
                continue
            vals = df[df['stage'] == stage]['duration_s'].values
            if len(vals) > 0:
                means.append(vals.mean())
                stds.append(vals.std())
                labels.append(r['label'])
        if means:
            x = range(len(means))
            ax.bar(x, means, yerr=stds, capsize=4)
            ax.set_xticks(list(x))
            ax.set_xticklabels(labels, rotation=45, ha='right')
        ax.set_title(title)
        ax.set_ylabel('average duration (s)')
    fig.suptitle('Confront run')
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, 'summary.png'), dpi=150)
    plt.close(fig)


def main():
    base_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    out_dir = sys.argv[2] if len(sys.argv) > 2 else 'plots'
    os.makedirs(out_dir, exist_ok=True)

    run_dirs = find_runs(base_dir)

    print('Found %d run in %s' % (len(run_dirs), base_dir))
    runs = []
    for rd in run_dirs:
        print('Run: %s' % rd)
        runs.append(load_run(rd))

    print('Plotting stream_stages.png')
    plot_stream_stages(runs, out_dir)
    print('Plotting consumer_stages.png')
    plot_consumer_stages(runs, out_dir)
    print('Plotting lag.png')
    plot_lag(runs, out_dir)
    print('Plotting resources_local.png resources_workers.png')
    plot_resources(runs, out_dir)
    print('Plotting summary.png')
    plot_summary_bars(runs, out_dir)

    print('Plots saved in %s' % out_dir)


if __name__ == '__main__':
    main()
