"""Statistics and histogram utilities for PostgreSQL benchmarking tools."""

import math
import time
from dataclasses import dataclass, field

import click


@dataclass
class BenchmarkStats:
    """Base statistics for benchmarks."""

    total_operations: int = 0
    failed_operations: int = 0
    latencies: list[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_report_time: float = field(default_factory=time.time)
    last_report_operations: int = 0


def calculate_percentiles(
    sorted_latencies: list[float], percentiles: list[float] = None
) -> dict:
    """Calculate percentiles for a sorted list of latencies.

    Args:
        sorted_latencies: Pre-sorted list of latency values
        percentiles: List of percentiles to calculate (default: [50, 75, 90, 95, 99, 99.9])

    Returns:
        Dictionary mapping percentile to value
    """
    if not sorted_latencies:
        return {}

    if percentiles is None:
        percentiles = [50, 75, 90, 95, 99, 99.9]

    result = {}
    for p in percentiles:
        idx = int(len(sorted_latencies) * p / 100)
        idx = min(idx, len(sorted_latencies) - 1)
        result[f"p{p}"] = sorted_latencies[idx]

    return result


def print_percentiles(latencies: list[float]) -> None:
    """Print latency percentiles table.

    Args:
        latencies: List of latency values (will be sorted internally)
    """
    if not latencies:
        return

    sorted_latencies = sorted(latencies)
    percentiles = [50, 75, 90, 95, 99, 99.9]

    click.echo("\nLatency Percentiles (ms):")
    click.echo("-" * 40)

    for p in percentiles:
        idx = int(len(sorted_latencies) * p / 100)
        idx = min(idx, len(sorted_latencies) - 1)
        click.echo(f"  p{p:<5}: {sorted_latencies[idx]:>8.2f} ms")


def print_histogram(latencies: list[float], num_buckets: int = None) -> None:
    """Print a histogram of latencies.

    Args:
        latencies: List of latency values (will be sorted internally)
        num_buckets: Number of histogram buckets (auto-calculated if None)
    """
    if not latencies:
        return

    sorted_latencies = sorted(latencies)

    click.echo("\nLatency Distribution:")
    click.echo("-" * 60)

    min_val = sorted_latencies[0]
    max_val = sorted_latencies[-1]

    if min_val == max_val:
        click.echo(f"  All values: {min_val:.2f} ms")
        return

    if num_buckets is None:
        num_buckets = min(20, len(sorted_latencies) // 10)
        if num_buckets < 5:
            num_buckets = 5

    # Use logarithmic buckets for better distribution
    log_min = math.log10(max(min_val, 0.1))
    log_max = math.log10(max_val)
    log_step = (log_max - log_min) / num_buckets

    buckets = []
    for i in range(num_buckets):
        lower = 10 ** (log_min + i * log_step)
        upper = 10 ** (log_min + (i + 1) * log_step)
        count = sum(1 for lat in sorted_latencies if lower <= lat < upper)
        buckets.append((lower, upper, count))

    max_count = max(b[2] for b in buckets) if buckets else 1
    bar_width = 40

    for lower, upper, count in buckets:
        bar_len = int((count / max_count) * bar_width) if max_count > 0 else 0
        bar = "█" * bar_len
        percentage = (count / len(sorted_latencies)) * 100
        click.echo(
            f"  {lower:>7.1f} - {upper:>7.1f} ms: {bar:<{bar_width}} "
            f"{count:>6} ({percentage:>5.1f}%)"
        )


def print_latency_stats(latencies: list[float]) -> None:
    """Print comprehensive latency statistics.

    Args:
        latencies: List of latency values
    """
    if not latencies:
        return

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    click.echo("\nLatency Statistics:")
    click.echo(f"  Average:            {avg_latency:.2f} ms")
    click.echo(f"  Minimum:            {min_latency:.2f} ms")
    click.echo(f"  Maximum:            {max_latency:.2f} ms")

    print_percentiles(latencies)
    print_histogram(latencies)
