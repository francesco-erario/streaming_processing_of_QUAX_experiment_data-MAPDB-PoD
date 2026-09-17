import os
import sys
import glob
import warnings
from collections import defaultdict

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.patches import Patch

warnings.filterwarnings('ignore', category=FutureWarning) 
cmap = matplotlib.colormaps.get_cmap('plasma')
#cmap = cm.get_cmap('plasma')
baseline_color = cmap(0.2)
other_color    = cmap(0.6)

plt.rcParams.update({'font.size': 13, 'axes.titlesize': 14, 'axes.labelsize': 13, 'xtick.labelsize': 11, 'ytick.labelsize': 11, 'legend.fontsize': 11})

DEFAULTS = {
    'dask-mode': 'single',
    'n-workers': '1',
    'kafka-partitions': '4',
    'rate': '4.2',
    'max-in-flight': '2',
    'n-slices': '4',
    'n-consumers': '1',
}

# batch = duck_i + duck_q, each 8192x1024 float32 -> ~64MB total per batch,
# regardless of how many slices it gets split into for publishing
BATCH_BYTES = 2 * 8192 * 1024 * 4


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


def effective_config(args):
    cfg = dict(DEFAULTS)
    for k, v in args.items():
        if k in DEFAULTS:
            cfg[k] = str(v)
    return cfg


def find_runs(base_dir):
    runs = []
    for name in sorted(os.listdir(base_dir)):
        if 'offline_dask' in name.lower():
            continue
        path = os.path.join(base_dir, name)
        if os.path.isdir(path) and os.path.exists(os.path.join(path, 'command.txt')):
            runs.append(path)
    return runs


def load_run(run_dir):
    label = os.path.basename(run_dir.rstrip('/'))
    cmd_path = os.path.join(run_dir, 'command.txt')
    args = parse_command(cmd_path) if os.path.exists(cmd_path) else {}
    cfg = effective_config(args)

    stream_path = os.path.join(run_dir, 'stream_benchmarks.csv')
    stream = pd.read_csv(stream_path) if os.path.exists(stream_path) else pd.DataFrame()

    consumer_files = sorted(glob.glob(os.path.join(run_dir, 'consumer_benchmarks_*.csv')))
    consumer_frames = []
    for cf in consumer_files:
        df = pd.read_csv(cf)
        df['consumer_file'] = os.path.basename(cf)
        consumer_frames.append(df)
    consumer = pd.concat(consumer_frames, ignore_index=True) if consumer_frames else pd.DataFrame()

    lag_path = os.path.join(run_dir, 'lag.csv')
    lag = pd.read_csv(lag_path) if os.path.exists(lag_path) else pd.DataFrame()

    res_path = os.path.join(run_dir, 'resources.csv')
    resources = pd.read_csv(res_path) if os.path.exists(res_path) else pd.DataFrame()

    return {
        'dir': run_dir,
        'label': label,
        'args': args,
        'cfg': cfg,
        'group': cfg['dask-mode'],
        'is_baseline': 'baseline' in label.lower(),
        'stream': stream,
        'consumer': consumer,
        'lag': lag,
        'resources': resources,
    }


def compute_family(run, reference_cfg):
    if run['is_baseline']:
        return None
    keys = sorted(k for k in run['cfg'] if k != 'dask-mode' and run['cfg'][k] != reference_cfg.get(k))
    if not keys:
        return None
    return '+'.join(keys)


def boxplot_compat(ax, data, labels, **kwargs):
    try:
        return ax.boxplot(data, tick_labels=labels, **kwargs)
    except TypeError:
        return ax.boxplot(data, labels=labels, **kwargs)


def order_with_baseline_first(runs, baseline_label):
    return sorted(runs, key=lambda r: (r['label'] != baseline_label, r['label']))


def style_xticks(ax):
    ax.tick_params(axis='x', rotation=45)
    for tick in ax.get_xticklabels():
        tick.set_ha('right')


def colors_for(labels, baseline_label):
    return [baseline_color if l == baseline_label else other_color for l in labels]


def add_box_legend(fig, baseline_label):
    elements = [Patch(facecolor=other_color, edgecolor='black', label='other runs')]
    if baseline_label:
        elements.insert(0, Patch(facecolor=baseline_color, edgecolor='black', label='baseline (%s)' % baseline_label))
    fig.legend(handles=elements, loc='upper right', fontsize=11)
    fig.text(0.01, 0.005, "box = 25°-75° |  black line = median  |  ticks = min-max", fontsize=9, ha='left')


def add_bar_legend(fig, baseline_label):
    elements = [Patch(facecolor=other_color, edgecolor='black', label='altre run')]
    if baseline_label:
        elements.insert(0, Patch(facecolor=baseline_color, edgecolor='black', label='baseline (%s)' % baseline_label))
    fig.legend(handles=elements, loc='upper right', fontsize=11)
    fig.text(0.01, 0.005, "bar = media  |  error bar = standard deviation", fontsize=9, ha='left')


def plot_stage_grid(runs, stages, src_key, title, out_path, baseline_label):
    n = len(stages)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 5.5 * nrows + 0.4), squeeze=False)
    axes_flat = axes.flatten()
    ordered = order_with_baseline_first(runs, baseline_label)
    for idx, stage in enumerate(stages):
        ax = axes_flat[idx]
        data, labels = [], []
        for r in ordered:
            df = r[src_key]
            if df.empty:
                continue
            vals = df[df['stage'] == stage]['duration_s'].values
            if len(vals) > 0:
                data.append(vals)
                labels.append(r['label'])
        if data:
            bp = boxplot_compat(ax, data, labels, showfliers=False, patch_artist=True, medianprops=dict(color='black', linewidth=1.5))
            for patch, lbl in zip(bp['boxes'], labels):
                patch.set_facecolor(baseline_color if lbl == baseline_label else other_color)
        ax.set_title(stage)
        ax.set_ylabel('duration (s)')
        style_xticks(ax)
    for j in range(len(stages), len(axes_flat)):
        axes_flat[j].axis('off')
    fig.suptitle(title)
    add_box_legend(fig, baseline_label)
    fig.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_stream_stages(runs, out_path, baseline_label):
    stages = ['download_i', 'download_q', 'download_total', 'slicing', 'publish']
    plot_stage_grid(runs, stages, 'stream', 'Producer (stream_benchmarks) - duration per stage', out_path, baseline_label)


def plot_consumer_stages(runs, out_path, baseline_label):
    stages = ['dask_submit', 'dask_compute', 'end_to_end_latency']
    plot_stage_grid(runs, stages, 'consumer', 'Consumer (dask) - duration per stage', out_path, baseline_label)


def compute_throughput_mbps(df, stage):
    sub = df[df['stage'] == stage]
    if sub.empty:
        return None
    durations = sub['duration_s'].values
    return (BATCH_BYTES / durations) / (1024 * 1024)


def plot_throughput_stages(runs, out_path, baseline_label):
    stages = ['download_total', 'publish', 'batch_total']
    ncols = len(stages)
    fig, axes = plt.subplots(1, ncols, figsize=(5.5 * ncols, 5.9), squeeze=False)
    axes_flat = axes.flatten()
    ordered = order_with_baseline_first(runs, baseline_label)
    for idx, stage in enumerate(stages):
        ax = axes_flat[idx]
        data, labels = [], []
        for r in ordered:
            df = r['stream']
            if df.empty:
                continue
            vals = compute_throughput_mbps(df, stage)
            if vals is not None and len(vals) > 0:
                data.append(vals)
                labels.append(r['label'])
        if data:
            bp = boxplot_compat(ax, data, labels, showfliers=False, patch_artist=True, medianprops=dict(color='black', linewidth=1.5))
            for patch, lbl in zip(bp['boxes'], labels):
                patch.set_facecolor(baseline_color if lbl == baseline_label else other_color)
        ax.set_title(stage)
        ax.set_ylabel('throughput (MB/s)')
        style_xticks(ax)
    fig.suptitle('Producer throughput per stage (batch = duck_i + duck_q, ~64MB total, independent of n-slices)')
    add_box_legend(fig, baseline_label)
    fig.tight_layout(rect=[0, 0.03, 1, 0.94])
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_lag(runs, out_path, baseline_label):
    fig, ax = plt.subplots(figsize=(10, 6))
    has_data = False
    for r in order_with_baseline_first(runs, baseline_label):
        df = r['lag']
        if df.empty:
            continue
        df = df.dropna(subset=['lag'])
        if df.empty:
            continue
        grouped = df.groupby('timestamp')['lag'].sum().reset_index()
        t0 = grouped['timestamp'].min()
        style = {'linewidth': 2.5, 'linestyle': '--', 'color': 'black'} if r['label'] == baseline_label else {}
        ax.plot(grouped['timestamp'] - t0, grouped['lag'], label=r['label'], **style)
        has_data = True
    ax.set_xlabel('time since run started (s)')
    ax.set_ylabel('total lag')
    ax.set_title('Kafka lag')
    if has_data:
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=11, borderaxespad=0)
    fig.tight_layout(rect=[0, 0, 0.8, 1])
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_resources(runs, out_dir, baseline_label):
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    ax_cpu, ax_mem = axes
    for r in order_with_baseline_first(runs, baseline_label):
        df = r['resources']
        if df.empty:
            continue
        local = df[df['location'] == 'local']
        if local.empty:
            continue
        t0 = local['timestamp'].min()
        style = {'linewidth': 2.5, 'linestyle': '--', 'color': 'black'} if r['label'] == baseline_label else {}
        ax_cpu.plot(local['timestamp'] - t0, local['cpu_percent'], label=r['label'], **style)
        ax_mem.plot(local['timestamp'] - t0, local['mem_percent'], label=r['label'], **style)
    ax_cpu.set_title('CPU client')
    ax_cpu.set_xlabel('time (s)')
    ax_cpu.set_ylabel('cpu %')
    ax_mem.set_title('Memory client')
    ax_mem.set_xlabel('time (s)')
    ax_mem.set_ylabel('RAM mem %')
    ax_mem.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=11, borderaxespad=0)
    fig.tight_layout(rect=[0, 0, 0.85, 1])
    fig.savefig(os.path.join(out_dir, 'resources_local.png'), dpi=150)
    plt.close(fig)

    fig2, axes2 = plt.subplots(1, 2, figsize=(15, 6))
    ax_cpu2, ax_mem2 = axes2
    for r in order_with_baseline_first(runs, baseline_label):
        df = r['resources']
        if df.empty:
            continue
        workers = df[df['location'] != 'local']
        if workers.empty:
            continue
        avg = workers.groupby('timestamp')[['cpu_percent', 'mem_percent']].mean().reset_index()
        t0 = avg['timestamp'].min()
        style = {'linewidth': 2.5, 'linestyle': '--', 'color': 'black'} if r['label'] == baseline_label else {}
        ax_cpu2.plot(avg['timestamp'] - t0, avg['cpu_percent'], label=r['label'], **style)
        ax_mem2.plot(avg['timestamp'] - t0, avg['mem_percent'], label=r['label'], **style)
    ax_cpu2.set_title('Average CPU worker Dask')
    ax_cpu2.set_xlabel('time (s)')
    ax_cpu2.set_ylabel('cpu %')
    ax_mem2.set_title('Average memory worker Dask')
    ax_mem2.set_xlabel('time (s)')
    ax_mem2.set_ylabel('mem %')
    ax_mem2.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=11, borderaxespad=0)
    fig2.tight_layout(rect=[0, 0, 0.85, 1])
    fig2.savefig(os.path.join(out_dir, 'resources_workers.png'), dpi=150)
    plt.close(fig2)


def plot_summary_bars(runs, out_path, baseline_label, title):
    metrics = [
        ('stream', 'download_total', 'Total download (producer)'),
        ('stream', 'publish', 'Publish Kafka (producer)'),
        ('consumer', 'end_to_end_latency', 'End-to-end latency (consumer)'),
    ]
    ordered = order_with_baseline_first(runs, baseline_label)
    width = max(15, 1.3 * len(ordered) * len(metrics) / 3)
    fig, axes = plt.subplots(1, 3, figsize=(width, 6.3))
    for ax, (src, stage, mtitle) in zip(axes, metrics):
        means, stds, labels = [], [], []
        for r in ordered:
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
            ax.bar(x, means, yerr=stds, capsize=4, color=colors_for(labels, baseline_label))
            ax.set_xticks(list(x))
            ax.set_xticklabels(labels)
        ax.set_title(mtitle)
        ax.set_ylabel('average duration (s)')
        style_xticks(ax)
    fig.suptitle(title)
    add_bar_legend(fig, baseline_label)
    fig.tight_layout(rect=[0, 0.03, 1, 0.94])
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def make_plot_set(runs, out_dir, baseline_label, summary_title):
    os.makedirs(out_dir, exist_ok=True)
    plot_stream_stages(runs, os.path.join(out_dir, 'stream_stages.png'), baseline_label)
    plot_consumer_stages(runs, os.path.join(out_dir, 'consumer_stages.png'), baseline_label)
    plot_throughput_stages(runs, os.path.join(out_dir, 'throughput.png'), baseline_label)
    plot_summary_bars(runs, os.path.join(out_dir, 'summary.png'), baseline_label, summary_title)
    plot_lag(runs, os.path.join(out_dir, 'lag.png'), baseline_label)
    plot_resources(runs, out_dir, baseline_label)


def main():
    base_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    out_dir  = sys.argv[2] if len(sys.argv) > 2 else 'plots'
    os.makedirs(out_dir, exist_ok=True)

    run_dirs = find_runs(base_dir)
    if not run_dirs:
        print('No run in %s' % base_dir)
        return

    print('Found %d run in %s' % (len(run_dirs), base_dir))
    runs = []
    for rd in run_dirs:
        print('Load run: %s' % rd)
        runs.append(load_run(rd))

    runs_by_group = defaultdict(list)
    for r in runs:
        runs_by_group[r['group']].append(r)

    for group, group_runs in runs_by_group.items():
        print('\n Group: %s (%d run) ' % (group, len(group_runs)))

        baseline_candidates = [r for r in group_runs if r['is_baseline']]
        if len(baseline_candidates) == 0:
            print('ATTENTION: no run with "baseline" in the name for %s' % group)
            baseline_run = None
            reference_cfg = DEFAULTS
        else:
            baseline_run = baseline_candidates[0]
            reference_cfg = baseline_run['cfg']
            print('Baseline: %s' % baseline_run['label'])

        baseline_label = baseline_run['label'] if baseline_run else None

        group_out = os.path.join(out_dir, group)
        make_plot_set(group_runs, group_out, baseline_label, 'All runs vs baseline')

        families = defaultdict(list)
        for r in group_runs:
            fam = compute_family(r, reference_cfg)
            if fam is not None:
                families[fam].append(r)

        for family, family_runs in families.items():
            members = ([baseline_run] if baseline_run else []) + family_runs
            print('Parameter family "%s": baseline=%s + %s' % (family, baseline_label, [r['label'] for r in family_runs]))
            family_out = os.path.join(group_out, 'families', family)
            make_plot_set(members, family_out, baseline_label, 'Baseline vs variation of "%s"' % family)

    print('\n Plots saved in %s' % out_dir)


if __name__ == '__main__':
    main()