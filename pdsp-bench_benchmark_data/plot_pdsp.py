#!/usr/bin/env python3
"""
Generic plotting utility for PDSP_Bench data.csv

Expected columns in data.csv:
- framework (flink, storm)
- query
- parallelism
- parallelism_source
- parallelism_avg
- parallelism_sink
- class_num_distinct
- producer_rate
- hardware_type
- event_rate_50, event_rate_95, event_rate_98
- throughput_50, throughput_95, throughput_98
- endtoend_latency
- cpu_50, cpu_95, cpu_98
- memory_50, memory_95, memory_98
- network_50, network_95, network_98
- path, artifact, count
"""

import argparse
import os
from typing import List, Optional

import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# Metric & styling configuration
# -----------------------------

METRICS = [
    "event_rate_50", "event_rate_95", "event_rate_98",
    "throughput_50", "throughput_95", "throughput_98",
    "endtoend_latency",
    "cpu_50", "cpu_95", "cpu_98",
    "memory_50", "memory_95", "memory_98",
    "network_50", "network_95", "network_98",
]

# Framework visual differentiation (via alpha)
FRAMEWORK_ALPHA = {
    "flink": 1.0,
    "storm": 0.55,
}

# Query-specific colors (stable across all plots)
QUERY_COLORS = {
    "Smart Grid": "#1b9e77",
    "Word Count": "#d95f02",
    "Word Count Shuffle": "#7570b3",
    "Ad Analytics": "#e7298a",
    "Google Cloud Monitoring": "#66a61e",
    "Sentiment Analysis": "#e6ab02",
    "Spike Detection": "#a6761d",
    "Log Processing": "#1f78b4",
    "Trending Topics": "#b2df8a",
    "Bargain Index": "#fb9a99",
    "Click Analytics": "#cab2d6",
    "Click Analytics Count-based": "#fdbf6f",
    "Machine Outlier": "#a6cee3",
    "Linear Road": "#ff7f00",
    "TPCH": "#6a3d9a",
    "TPCH Window": "#b15928",
    "Traffic Monitoring": "#8dd3c7",
}

# Query-specific hatching (stable across all plots; also helps in grayscale)
QUERY_HATCHES = {
    "Smart Grid": "///",
    "Word Count": "\\\\",
    "Word Count Shuffle": "...",
    "Ad Analytics": "***",
    "Google Cloud Monitoring": "xxx",
    "Sentiment Analysis": "++",
    "Spike Detection": "oo",
    "Log Processing": "//",
    "Trending Topics": "..",
    "Bargain Index": "\\",
    "Click Analytics": "++",
    "Click Analytics Count-based": "**",
    "Machine Outlier": "xx",
    "Linear Road": "--",
    "TPCH": "oo",
    "TPCH Window": "OO",
    "Traffic Monitoring": "||",
}


# -----------------------------
# Global aesthetics
# -----------------------------

def configure_style():
    """Configure a clean, publication-friendly matplotlib style."""
    plt.rcParams.update({
        "figure.figsize": (7, 4),

        # base font
        "font.size": 11,

        # axes / titles / legend
        "axes.titlesize": 13,
        "axes.labelsize": 12,
        "legend.fontsize": 10,

        # ticks
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,

        "figure.autolayout": True,
    })


def add_value_labels(ax, spacing: int = 3, fontsize: int = 9):
    """
    Add value labels above each bar in all bar containers in the given Axes.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axes containing the bar plot.
    spacing : int
        Distance in points between bar top and label.
    fontsize : int
        Font size for the labels.
    """
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            # skip empty / zero-height bars
            if height is None or height == 0:
                continue
            ax.annotate(
                f"{height:.1f}",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, spacing),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=fontsize,
            )


# -----------------------------
# Data loading & filtering
# -----------------------------

def load_data(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)


def filter_data(
    df: pd.DataFrame,
    query: Optional[str] = None,
    hardware_type: Optional[str] = None,
    frameworks: Optional[List[str]] = None,
    producer_rate: Optional[int] = None,
) -> pd.DataFrame:
    """Filter dataframe by query, hardware_type, frameworks, producer_rate."""
    sub = df.copy()

    if query is not None:
        sub = sub[sub["query"] == query]

    if hardware_type is not None:
        sub = sub[sub["hardware_type"] == hardware_type]

    if frameworks:
        sub = sub[sub["framework"].isin(frameworks)]

    if producer_rate is not None:
        sub = sub[sub["producer_rate"] == producer_rate]

    return sub


# -----------------------------
# File helpers
# -----------------------------

def _ensure_dir_for_file(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _save_figure_multi(fig, save_path: str, all_formats: bool):
    """
    Save figure:
      - always as PNG (base_name.png)
      - if all_formats=True, also PDF and SVG.

    If save_path has an extension, it's stripped and used as base name.
    """
    if not save_path:
        return

    _ensure_dir_for_file(save_path)
    root, _ext = os.path.splitext(save_path)
    if not root:
        root = save_path

    png_path = root + ".png"
    fig.savefig(png_path, dpi=300)

    if all_formats:
        fig.savefig(root + ".pdf")
        fig.savefig(root + ".svg")


# -----------------------------
# Plotting functions
# -----------------------------

def plot_scaling_single_framework(
    df: pd.DataFrame,
    framework: str,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
):
    """
    Framework internal scaling:
    X = parallelism_avg, Y = metric, filtered to one framework.
    """
    df_fw = df[df["framework"] == framework]
    if df_fw.empty:
        raise ValueError(f"No data for framework={framework} after filtering.")

    if metric not in df_fw.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    group = df_fw.groupby("parallelism_avg")[metric]

    if agg == "mean":
        grouped = group.mean().reset_index()
    elif agg == "median":
        grouped = group.median().reset_index()
    elif agg == "max":
        grouped = group.max().reset_index()
    elif agg == "min":
        grouped = group.min().reset_index()
    else:
        raise ValueError(f"Unsupported agg='{agg}'")

    grouped = grouped.sort_values("parallelism_avg")

    fig, ax = plt.subplots()

    # Styling: query color + query hatch (assumes single-query slice; if multiple, first query)
    query = df_fw["query"].iloc[0] if "query" in df_fw.columns and not df_fw["query"].isna().all() else None
    color = QUERY_COLORS.get(query, "#333333") if query is not None else "#333333"
    hatch = QUERY_HATCHES.get(query, "") if query is not None else ""

    ax.bar(
        grouped["parallelism_avg"].astype(str),
        grouped[metric],
        color=color,
        edgecolor="black",
        hatch=hatch,
    )

    ax.set_xlabel("parallelism_avg")
    ax.set_ylabel(metric)

    if title is None:
        if query:
            title = f"{query} | {framework}: {metric} vs parallelism_avg ({agg})"
        else:
            title = f"{framework}: {metric} vs parallelism_avg ({agg})"
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # add labels
    add_value_labels(ax)

    plt.tight_layout()
    _save_figure_multi(fig, save_path, all_formats)

    if show:
        plt.show()
    else:
        plt.close(fig)


def plot_scaling_compare_frameworks(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
):
    """
    Flink vs Storm comparison:
    X = parallelism_avg, grouped bars = framework, Y = metric.
    Assumes df already filtered by query, hardware_type, producer_rate.
    """
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")
    if "framework" not in df.columns:
        raise ValueError("DataFrame must contain 'framework' column.")
    if "parallelism_avg" not in df.columns:
        raise ValueError("DataFrame must contain 'parallelism_avg' column.")

    group = df.groupby(["framework", "parallelism_avg"])[metric]

    if agg == "mean":
        grouped = group.mean().reset_index()
    elif agg == "median":
        grouped = group.median().reset_index()
    elif agg == "max":
        grouped = group.max().reset_index()
    elif agg == "min":
        grouped = group.min().reset_index()
    else:
        raise ValueError(f"Unsupported agg='{agg}'")

    pivot = grouped.pivot(index="parallelism_avg", columns="framework", values=metric)
    pivot = pivot.sort_index()

    fig, ax = plt.subplots()
    pivot.plot(kind="bar", ax=ax)

    # In normal usage, df is filtered to a single query
    query = df["query"].iloc[0] if "query" in df.columns and not df["query"].isna().all() else None
    color = QUERY_COLORS.get(query, "#333333") if query is not None else "#333333"
    hatch = QUERY_HATCHES.get(query, "") if query is not None else ""

    # Same query color/hatch; frameworks are differentiated by alpha
    for container in ax.containers:
        fw_label = container.get_label()
        alpha = FRAMEWORK_ALPHA.get(str(fw_label).lower(), 0.8)
        for bar in container:
            bar.set_edgecolor("black")
            bar.set_color(color)
            bar.set_hatch(hatch)
            bar.set_alpha(alpha)

    ax.set_xlabel("parallelism_avg")
    ax.set_ylabel(metric)

    if title is None:
        if query:
            title = f"{query}: {metric} vs parallelism_avg (Flink vs Storm, {agg})"
        else:
            title = f"{metric} vs parallelism_avg (Flink vs Storm, {agg})"
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="framework")

    add_value_labels(ax)

    plt.tight_layout()
    _save_figure_multi(fig, save_path, all_formats)

    if show:
        plt.show()
    else:
        plt.close(fig)


def plot_queries_single_framework(
    df: pd.DataFrame,
    framework: str,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
):
    """
    All queries in one plot for a single framework.
    X = query, Y = metric (aggregated over runs & parallelism_avg).
    """
    df_fw = df[df["framework"] == framework]
    if df_fw.empty:
        raise ValueError(f"No data for framework={framework} after filtering.")

    if metric not in df_fw.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    group = df_fw.groupby("query")[metric]

    if agg == "mean":
        grouped = group.mean().reset_index()
    elif agg == "median":
        grouped = group.median().reset_index()
    elif agg == "max":
        grouped = group.max().reset_index()
    elif agg == "min":
        grouped = group.min().reset_index()
    else:
        raise ValueError(f"Unsupported agg='{agg}'")

    grouped = grouped.sort_values("query")

    fig, ax = plt.subplots()

    x = range(len(grouped))
    bars = ax.bar(x, grouped[metric])

    for bar, q in zip(bars, grouped["query"]):
        bar.set_edgecolor("black")
        bar.set_color(QUERY_COLORS.get(q, "#333333"))
        bar.set_hatch(QUERY_HATCHES.get(q, ""))

    ax.set_xlabel("query")
    ax.set_ylabel(metric)
    if title is None:
        title = f"{framework}: {metric} by query ({agg})"
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_xticks(x)
    ax.set_xticklabels(grouped["query"], rotation=45, ha="right")

    add_value_labels(ax)

    plt.tight_layout()
    _save_figure_multi(fig, save_path, all_formats)

    if show:
        plt.show()
    else:
        plt.close(fig)


def plot_queries_compare_frameworks(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
):
    """
    All queries in one plot, comparing frameworks.
    X = query, grouped bars = framework, Y = metric.

    Same query color/hatch for both frameworks; Flink vs Storm distinguished by alpha.
    """
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")
    if "framework" not in df.columns:
        raise ValueError("DataFrame must contain 'framework' column.")
    if "query" not in df.columns:
        raise ValueError("DataFrame must contain 'query' column.")

    group = df.groupby(["query", "framework"])[metric]

    if agg == "mean":
        grouped = group.mean().reset_index()
    elif agg == "median":
        grouped = group.median().reset_index()
    elif agg == "max":
        grouped = group.max().reset_index()
    elif agg == "min":
        grouped = group.min().reset_index()
    else:
        raise ValueError(f"Unsupported agg='{agg}'")

    pivot = grouped.pivot(index="query", columns="framework", values=metric)
    pivot = pivot.sort_index()

    fig, ax = plt.subplots()
    pivot.plot(kind="bar", ax=ax)

    queries = list(pivot.index)

    # container per framework; bar per query
    for container in ax.containers:
        fw_label = container.get_label()
        alpha = FRAMEWORK_ALPHA.get(str(fw_label).lower(), 0.8)
        for bar, q in zip(container, queries):
            bar.set_edgecolor("black")
            bar.set_color(QUERY_COLORS.get(q, "#333333"))
            bar.set_hatch(QUERY_HATCHES.get(q, ""))
            bar.set_alpha(alpha)

    ax.set_xlabel("query")
    ax.set_ylabel(metric)
    if title is None:
        title = f"{metric} by query (framework comparison, {agg})"
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.legend(title="framework")

    add_value_labels(ax)

    plt.tight_layout()
    _save_figure_multi(fig, save_path, all_formats)

    if show:
        plt.show()
    else:
        plt.close(fig)


def batch_compare_frameworks_over_event_rates(
    df: pd.DataFrame,
    metric: str,
    agg: str,
    out_dir: str,
    show: bool,
    all_formats: bool,
):
    """
    For given df (already filtered by query, hardware_type, frameworks),
    loop over all producer_rate values and generate
    Flink vs Storm scaling plots for each producer_rate.

    Files go to:
        {out_dir}/compare_flink_storm/<query>/<hardware_type>/<producer_rate>_{metric}.png
    """
    if df.empty:
        raise ValueError("No data left for batch plotting.")

    query_values = df["query"].dropna().unique().tolist()
    hw_values = df["hardware_type"].dropna().unique().tolist()

    for q in query_values:
        for hw in hw_values:
            df_q_hw = df[(df["query"] == q) & (df["hardware_type"] == hw)]
            if df_q_hw.empty:
                continue

            producer_rates = sorted(df_q_hw["producer_rate"].dropna().unique().tolist())
            for pr in producer_rates:
                df_q_hw_pr = df_q_hw[df_q_hw["producer_rate"] == pr]

                if df_q_hw_pr["framework"].nunique() < 2:
                    print(f"[WARN] Only one framework for {q}, {hw}, pr={pr}, skipping.")
                    continue

                title = f"{q} | {hw} | rate={pr} | {metric}"
                save_base = os.path.join(
                    out_dir,
                    "compare_flink_storm",
                    q.replace(" ", "_"),
                    hw,
                    f"{pr}_{metric}",
                )
                print(f"[INFO] Plotting {title} -> {save_base}")
                plot_scaling_compare_frameworks(
                    df_q_hw_pr,
                    metric=metric,
                    agg=agg,
                    title=title,
                    save_path=save_base,
                    show=show,
                    all_formats=all_formats,
                )


def batch_compare_frameworks_over_all_metrics_and_event_rates(
    df: pd.DataFrame,
    agg: str,
    out_dir: str,
    show: bool,
    all_formats: bool,
):
    """
    For the given df (already filtered by query, hardware_type, frameworks),
    loop over ALL METRICS and ALL producer_rate values, and generate
    Flink vs Storm scaling plots.

    Output structure:
        {out_dir}/compare_flink_storm_all_metrics/<query>/<hardware_type>/<metric>/<producer_rate>.png
    """
    if df.empty:
        raise ValueError("No data left for batch plotting.")

    query_values = df["query"].dropna().unique().tolist()
    hw_values = df["hardware_type"].dropna().unique().tolist()

    for q in query_values:
        for hw in hw_values:
            df_q_hw = df[(df["query"] == q) & (df["hardware_type"] == hw)]
            if df_q_hw.empty:
                continue

            producer_rates = sorted(df_q_hw["producer_rate"].dropna().unique().tolist())

            for metric in METRICS:
                if metric not in df_q_hw.columns:
                    print(f"[WARN] Metric '{metric}' missing for {q}, {hw}, skipping.")
                    continue

                for pr in producer_rates:
                    df_slice = df_q_hw[df_q_hw["producer_rate"] == pr]
                    if df_slice["framework"].nunique() < 2:
                        print(f"[WARN] Only one framework for {q}, {hw}, rate={pr}, metric={metric}, skipping.")
                        continue

                    title = f"{q} | {hw} | rate={pr} | {metric}"
                    save_base = os.path.join(
                        out_dir,
                        "compare_flink_storm_all_metrics",
                        q.replace(" ", "_"),
                        hw,
                        metric,
                        f"{pr}",
                    )
                    print(f"[INFO] Plotting {title} -> {save_base}")
                    plot_scaling_compare_frameworks(
                        df_slice,
                        metric=metric,
                        agg=agg,
                        title=title,
                        save_path=save_base,
                        show=show,
                        all_formats=all_formats,
                    )


# -----------------------------
# CLI
# -----------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="PDSP_Bench plotting utility (Flink/Storm, hardware, event rates)."
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="data.csv",
        help="Path to data.csv (default: data.csv)",
    )
    parser.add_argument(
        "--plot",
        type=str,
        required=True,
        choices=[
            "scaling_single",             # internal scaling for one framework
            "scaling_compare_fw",         # compare Flink vs Storm
            "queries_single_framework",   # all queries, one framework
            "queries_compare_frameworks", # all queries, compare frameworks
            "batch_compare_eventrates",   # loop over producer_rate (one metric)
            "batch_all_metrics_eventrates",  # loop over producer_rate AND all metrics
        ],
        help="Type of plot to generate.",
    )
    parser.add_argument(
        "--framework",
        type=str,
        default=None,
        help="Framework name (flink or storm) for scaling_single / queries_single_framework.",
    )
    parser.add_argument(
        "--frameworks",
        type=str,
        nargs="*",
        default=None,
        help="Frameworks to include (e.g. flink storm).",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Filter: query name (exact).",
    )
    parser.add_argument(
        "--hardware_type",
        type=str,
        default=None,
        help="Filter: hardware_type (e.g. c6525-100g).",
    )
    parser.add_argument(
        "--producer_rate",
        type=int,
        default=None,
        help="Filter: producer_rate (event rate). "
             "For batch_* modes, we loop over all rates and ignore this.",
    )
    parser.add_argument(
        "--metric",
        type=str,
        required=True,
        help="Metric to plot (e.g. throughput_50, endtoend_latency, cpu_50, ...). "
             "Ignored for batch_all_metrics_eventrates.",
    )
    parser.add_argument(
        "--agg",
        type=str,
        default="mean",
        choices=["mean", "median", "max", "min"],
        help="Aggregation for repeated runs (default: mean).",
    )
    parser.add_argument(
        "--save",
        type=str,
        default=None,
        help="Path to save a single figure (for non-batch modes). "
             "Extension is ignored; PNG is always saved, PDF/SVG if --all-formats.",
    )
    parser.add_argument(
        "--out_dir",
        type=str,
        default="plots",
        help="Base output directory for batch modes (default: plots).",
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Do not display figures interactively.",
    )
    parser.add_argument(
        "--all-formats",
        action="store_true",
        help="If set, save PNG + PDF + SVG for each figure (when saving).",
    )
    parser.add_argument(
        "--title",
        type=str,
        default=None,
        help="Custom plot title.",
    )
    return parser.parse_args()


def main():
    configure_style()
    args = parse_args()

    df = load_data(args.csv)

    # For batch modes, ignore --producer_rate and loop over all
    producer_rate = args.producer_rate
    if args.plot in {"batch_compare_eventrates", "batch_all_metrics_eventrates"}:
        producer_rate = None

    df_filtered = filter_data(
        df,
        query=args.query,
        hardware_type=args.hardware_type,
        frameworks=args.frameworks,
        producer_rate=producer_rate,
    )

    if df_filtered.empty:
        raise SystemExit(
            "No data left after filtering. "
            "Check query/hardware_type/frameworks/producer_rate."
        )

    show = not args.no_show
    all_formats = args.all_formats

    if args.plot == "scaling_single":
        if args.framework is None:
            raise SystemExit("You must provide --framework for scaling_single.")
        plot_scaling_single_framework(
            df_filtered,
            framework=args.framework,
            metric=args.metric,
            agg=args.agg,
            title=args.title,
            save_path=args.save,
            show=show,
            all_formats=all_formats,
        )

    elif args.plot == "scaling_compare_fw":
        plot_scaling_compare_frameworks(
            df_filtered,
            metric=args.metric,
            agg=args.agg,
            title=args.title,
            save_path=args.save,
            show=show,
            all_formats=all_formats,
        )

    elif args.plot == "queries_single_framework":
        if args.framework is None:
            raise SystemExit("You must provide --framework for queries_single_framework.")
        plot_queries_single_framework(
            df_filtered,
            framework=args.framework,
            metric=args.metric,
            agg=args.agg,
            title=args.title,
            save_path=args.save,
            show=show,
            all_formats=all_formats,
        )

    elif args.plot == "queries_compare_frameworks":
        plot_queries_compare_frameworks(
            df_filtered,
            metric=args.metric,
            agg=args.agg,
            title=args.title,
            save_path=args.save,
            show=show,
            all_formats=all_formats,
        )

    elif args.plot == "batch_compare_eventrates":
        batch_compare_frameworks_over_event_rates(
            df_filtered,
            metric=args.metric,
            agg=args.agg,
            out_dir=args.out_dir,
            show=show,
            all_formats=all_formats,
        )

    elif args.plot == "batch_all_metrics_eventrates":
        batch_compare_frameworks_over_all_metrics_and_event_rates(
            df_filtered,
            agg=args.agg,
            out_dir=args.out_dir,
            show=show,
            all_formats=all_formats,
        )


if __name__ == "__main__":
    main()
