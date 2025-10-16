#!/usr/bin/env python3
"""
Plot processing-time latencies from a CSV produced by the aggregator.

Expected columns (any missing ones are ignored gracefully):
  - timestamp (e.g. "2025-09-15 12:34:56")
  - avg_proc_time_us
  - p50_us
  - p90_us
  - p99_us
  - p99_9_us

Usage:
  python latencies.py --csv report.csv --out latencies.png --xmax 150 --logy
"""

import argparse
import sys
import pandas as pd
import matplotlib.pyplot as plt

DEFAULT_METRICS = [
    "avg_proc_time_us",
    "p50_us",
    "p90_us",
    "p99_us",
    "p99_9_us",
]


def main():
    ap = argparse.ArgumentParser(
        description="Plot per-message processing latencies from CSV."
    )
    ap.add_argument("--csv", required=True, help="Path to CSV file (e.g., report.csv)")
    ap.add_argument(
        "--out", default="latencies.png", help="Output image file (PNG/SVG)"
    )
    ap.add_argument("--timecol", default="timestamp", help="Timestamp column name")
    ap.add_argument(
        "--metrics",
        nargs="*",
        default=DEFAULT_METRICS,
        help=f"Metrics to plot (default: {', '.join(DEFAULT_METRICS)})",
    )
    ap.add_argument(
        "--xmax",
        type=float,
        default=None,
        help="Cap x-axis (seconds since start), e.g. 150",
    )
    ap.add_argument(
        "--logy", action="store_true", help="Use log scale for Y-axis (processing time)"
    )
    ap.add_argument(
        "--title",
        default="Per-message Processing Latency (avg & tails)",
        help="Plot title",
    )
    args = ap.parse_args()

    # Load CSV
    try:
        df = pd.read_csv(args.csv)
    except Exception as e:
        print(f"Failed to read CSV '{args.csv}': {e}", file=sys.stderr)
        sys.exit(1)

    if args.timecol not in df.columns:
        print(
            f"CSV missing time column '{args.timecol}'. Columns: {list(df.columns)}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Parse time and compute seconds since start
    df[args.timecol] = pd.to_datetime(df[args.timecol], errors="coerce")
    df = df.dropna(subset=[args.timecol]).sort_values(args.timecol)
    if df.empty:
        print("No valid timestamp rows after parsing.", file=sys.stderr)
        sys.exit(1)

    t0 = df[args.timecol].iloc[0]
    df["seconds_since_start"] = (df[args.timecol] - t0).dt.total_seconds()

    # Pick only the metrics that exist
    present = [m for m in args.metrics if m in df.columns]
    missing = [m for m in args.metrics if m not in df.columns]
    if not present:
        print(f"No requested metrics found. Missing: {missing}", file=sys.stderr)
        print(f"Available columns: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)
    if missing:
        print(f"Warning: missing metrics (will skip): {missing}", file=sys.stderr)

    # Plot
    plt.figure(figsize=(10, 6))
    x = df["seconds_since_start"]
    for m in present:
        plt.plot(x, df[m], label=m)

    plt.xlabel("Seconds Since Start")
    plt.ylabel("Processing Time (us)")
    plt.title(args.title)
    if args.xmax is not None:
        plt.xlim(0, args.xmax)
    if args.logy:
        plt.yscale("log")
    plt.grid(True, which="both", linestyle="--", alpha=0.6)
    plt.legend(title="Latency metrics")
    plt.tight_layout()
    plt.savefig(args.out, dpi=150)
    print(f"Saved plot to {args.out}")


if __name__ == "__main__":
    main()
