#!/usr/bin/env python3
"""PostgreSQL connection throughput benchmarking tool."""

import asyncio
import json
import math
import resource
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import asyncpg
import click
import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend
import matplotlib.pyplot as plt


@dataclass
class BenchmarkStats:
    """Statistics for the benchmark."""

    total_connections: int = 0
    failed_connections: int = 0
    latencies: list[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_report_time: float = field(default_factory=time.time)
    last_report_connections: int = 0


class ConnectionBenchmark:
    """Benchmarks PostgreSQL connection creation/closing throughput."""

    def __init__(self, postgres_urls: list[str], concurrency: int):
        self.postgres_urls = postgres_urls
        self.concurrency = concurrency
        self.shutdown = False
        self.stats = BenchmarkStats()
        self.stats_lock = asyncio.Lock()

    async def worker(self, worker_id: int) -> None:
        """Worker that continuously opens and closes connections."""
        # Assign this worker to a specific URL using round-robin
        url_index = worker_id % len(self.postgres_urls)
        postgres_url = self.postgres_urls[url_index]
        
        while not self.shutdown:
            try:
                start = time.perf_counter()

                conn = await asyncpg.connect(postgres_url, timeout=30)
                await conn.execute("SELECT 1")
                await conn.close()

                latency = (time.perf_counter() - start) * 1000

                async with self.stats_lock:
                    self.stats.total_connections += 1
                    self.stats.latencies.append(latency)

            except asyncio.CancelledError:
                break
            except Exception as e:
                async with self.stats_lock:
                    self.stats.failed_connections += 1
                if not self.shutdown:
                    if isinstance(e, asyncpg.exceptions.TooManyConnectionsError):
                        await asyncio.sleep(0.1)
                    elif isinstance(e, OSError) and e.errno == 24:
                        await asyncio.sleep(0.1)
                    else:
                        await asyncio.sleep(0.01)

    async def reporter(self) -> None:
        """Reports statistics every second."""
        await asyncio.sleep(1)

        while not self.shutdown:
            async with self.stats_lock:
                current_time = time.time()
                time_delta = current_time - self.stats.last_report_time
                conn_delta = (
                    self.stats.total_connections - self.stats.last_report_connections
                )

                if time_delta > 0:
                    rate = conn_delta / time_delta

                    if self.stats.latencies:
                        recent_latencies = self.stats.latencies[
                            -min(1000, len(self.stats.latencies)) :
                        ]
                        avg_latency = sum(recent_latencies) / len(recent_latencies)
                        min_latency = min(recent_latencies)
                        max_latency = max(recent_latencies)

                        click.echo(
                            f"[{int(current_time - self.stats.start_time):>4}s] "
                            f"Rate: {rate:>7.1f} conn/s | "
                            f"Total: {self.stats.total_connections:>7} | "
                            f"Failed: {self.stats.failed_connections:>5} | "
                            f"Latency (ms): avg={avg_latency:>6.1f} "
                            f"min={min_latency:>6.1f} max={max_latency:>6.1f}"
                        )
                    else:
                        click.echo(
                            f"[{int(current_time - self.stats.start_time):>4}s] "
                            f"Rate: {rate:>7.1f} conn/s | "
                            f"Total: {self.stats.total_connections:>7} | "
                            f"Failed: {self.stats.failed_connections:>5}"
                        )

                self.stats.last_report_time = current_time
                self.stats.last_report_connections = self.stats.total_connections

            await asyncio.sleep(1)

    def print_histogram(self) -> None:
        """Print a histogram of connection latencies."""
        if not self.stats.latencies:
            return

        sorted_latencies = sorted(self.stats.latencies)

        percentiles = [50, 75, 90, 95, 99, 99.9]
        click.echo("\nLatency Percentiles (ms):")
        click.echo("-" * 40)
        for p in percentiles:
            idx = int(len(sorted_latencies) * p / 100)
            idx = min(idx, len(sorted_latencies) - 1)
            click.echo(f"  p{p:<5}: {sorted_latencies[idx]:>8.2f} ms")

        click.echo("\nLatency Distribution:")
        click.echo("-" * 60)

        min_val = sorted_latencies[0]
        max_val = sorted_latencies[-1]

        if min_val == max_val:
            click.echo(f"  All values: {min_val:.2f} ms")
            return

        num_buckets = min(20, len(sorted_latencies) // 10)
        if num_buckets < 5:
            num_buckets = 5

        log_min = math.log10(max(min_val, 0.1))
        log_max = math.log10(max_val)
        log_step = (log_max - log_min) / num_buckets

        buckets = []
        for i in range(num_buckets):
            lower = 10 ** (log_min + i * log_step)
            upper = 10 ** (log_min + (i + 1) * log_step)
            count = sum(1 for lat in sorted_latencies if lower <= lat < upper)
            buckets.append((lower, upper, count))

        max_count = max(b[2] for b in buckets)
        bar_width = 40

        for lower, upper, count in buckets:
            bar_len = int((count / max_count) * bar_width) if max_count > 0 else 0
            bar = "█" * bar_len
            percentage = (count / len(sorted_latencies)) * 100
            click.echo(
                f"  {lower:>7.1f} - {upper:>7.1f} ms: {bar:<{bar_width}} "
                f"{count:>6} ({percentage:>5.1f}%)"
            )

    def print_summary(self) -> None:
        """Print final summary statistics."""
        duration = time.time() - self.stats.start_time

        click.echo("\n" + "=" * 60)
        click.echo("Benchmark Summary")
        click.echo("=" * 60)

        click.echo(f"Duration:             {duration:.1f} seconds")
        click.echo(f"Concurrency:          {self.concurrency} workers")
        click.echo(f"Total Connections:    {self.stats.total_connections}")
        click.echo(f"Failed Connections:   {self.stats.failed_connections}")

        if self.stats.total_connections > 0:
            success_rate = (
                (self.stats.total_connections - self.stats.failed_connections)
                / self.stats.total_connections
                * 100
            )
            click.echo(f"Success Rate:         {success_rate:.1f}%")

        if duration > 0:
            avg_rate = self.stats.total_connections / duration
            click.echo(f"Average Rate:         {avg_rate:.1f} connections/second")

        if self.stats.latencies:
            avg_latency = sum(self.stats.latencies) / len(self.stats.latencies)
            min_latency = min(self.stats.latencies)
            max_latency = max(self.stats.latencies)

            click.echo("\nLatency Statistics:")
            click.echo(f"  Average:            {avg_latency:.2f} ms")
            click.echo(f"  Minimum:            {min_latency:.2f} ms")
            click.echo(f"  Maximum:            {max_latency:.2f} ms")

            self.print_histogram()


async def run_single_benchmark(
    urls: list[str], concurrency: int, duration: int, quiet: bool = False
) -> tuple[float, float, list[float]]:
    """Run a single benchmark at a specific concurrency level.

    Returns:
        Tuple of (average connections per second, average latency in ms, latency samples)
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    needed = concurrency * 4 + 1000

    if soft < needed:
        try:
            new_limit = min(needed, hard)
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_limit, hard))
            if not quiet:
                click.echo(
                    f"Increased file descriptor limit from {soft} to {new_limit}"
                )
        except Exception as e:
            if not quiet:
                click.echo(
                    f"Warning: Could not increase file descriptor limit: {e}", err=True
                )

    if not quiet:
        click.echo("PostgreSQL Connection Throughput Benchmark")
        click.echo("=" * 60)
        targets = [url.split('@')[-1] if '@' in url else url for url in urls]
        if len(targets) == 1:
            click.echo(f"Target:       {targets[0]}")
        else:
            click.echo(f"Targets ({len(targets)} servers):")
            for i, target in enumerate(targets, 1):
                click.echo(f"  {i}. {target}")
        click.echo(f"Concurrency:  {concurrency} workers")
        duration_str = "indefinite" if duration == 0 else f"{duration} seconds"
        click.echo(f"Duration:     {duration_str}")
        click.echo(f"FD Limit:     {resource.getrlimit(resource.RLIMIT_NOFILE)[0]}")
        click.echo("=" * 60)
        click.echo()

    benchmark = ConnectionBenchmark(urls, concurrency)

    loop = asyncio.get_event_loop()

    def signal_handler(sig):
        click.echo("\n\nReceived interrupt signal, shutting down...")
        benchmark.shutdown = True
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    try:
        workers = [benchmark.worker(i) for i in range(concurrency)]
        reporter = benchmark.reporter()

        if duration > 0:
            # Create tasks for workers and reporter
            worker_tasks = [asyncio.create_task(w) for w in workers]
            reporter_task = asyncio.create_task(reporter)

            # Wait for the duration
            await asyncio.sleep(duration)

            # Signal shutdown
            benchmark.shutdown = True

            # Cancel all tasks
            for task in worker_tasks + [reporter_task]:
                task.cancel()

            # Wait for tasks to finish cancellation
            await asyncio.gather(*worker_tasks, reporter_task, return_exceptions=True)
        else:
            await asyncio.gather(*workers, reporter, return_exceptions=True)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        benchmark.shutdown = True
        await asyncio.sleep(0.1)
        if not quiet:
            benchmark.print_summary()

    # Calculate and return metrics after finally block
    duration = time.time() - benchmark.stats.start_time
    avg_rate = benchmark.stats.total_connections / duration if duration > 0 else 0
    avg_latency = (
        sum(benchmark.stats.latencies) / len(benchmark.stats.latencies)
        if benchmark.stats.latencies
        else 0
    )
    # Sample latencies for visualization (up to 10000 samples)
    latency_samples = (
        benchmark.stats.latencies[:10000]
        if len(benchmark.stats.latencies) > 10000
        else benchmark.stats.latencies.copy()
    )
    return avg_rate, avg_latency, latency_samples


def generate_power_of_2_range(min_val: int, max_val: int) -> list[int]:
    """Generate a list of powers of 2 between min_val and max_val."""
    values = []
    current = 1

    # Find the first power of 2 >= min_val
    while current < min_val:
        current *= 2

    # Collect all powers of 2 up to max_val
    while current <= max_val:
        values.append(current)
        current *= 2

    # If min_val is not a power of 2, include it
    if min_val not in values and min_val > 0:
        values.insert(0, min_val)

    # If max_val is not a power of 2, include it
    if max_val not in values and max_val > min_val:
        values.append(max_val)

    return sorted(values)


def save_json_results(
    concurrency_levels: list[int],
    connections_per_sec: list[float],
    avg_latencies: list[float],
    latency_samples: list[list[float]],
    concurrency_min: int,
    concurrency_max: int,
    name: Optional[str] = None,
    url: Optional[str] = None,
    duration: Optional[int] = None,
) -> str:
    """Save benchmark results as JSON.

    Returns:
        JSON filename
    """
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_bench_{timestamp_str}"
    if name:
        base_name += f"_{name}"
    base_name += f"_c{concurrency_min}-{concurrency_max}"
    json_filename = f"{base_name}.json"

    results = {
        "timestamp": timestamp.isoformat(),
        "name": name,
        "url": url.split("@")[-1] if url and "@" in url else url,
        "duration_per_run": duration,
        "concurrency_min": concurrency_min,
        "concurrency_max": concurrency_max,
        "results": [
            {
                "concurrency": c,
                "connections_per_sec": rate,
                "avg_latency_ms": latency,
                "latency_percentiles": {
                    "p50": float(sorted(samples)[len(samples) // 2]) if samples else 0,
                    "p95": float(sorted(samples)[int(len(samples) * 0.95)])
                    if samples
                    else 0,
                    "p99": float(sorted(samples)[int(len(samples) * 0.99)])
                    if samples
                    else 0,
                }
                if samples
                else {},
            }
            for c, rate, latency, samples in zip(
                concurrency_levels, connections_per_sec, avg_latencies, latency_samples
            )
        ],
    }

    with open(json_filename, "w") as f:
        json.dump(results, f, indent=2)

    return json_filename


def save_plots(
    concurrency_levels: list[int],
    connections_per_sec: list[float],
    avg_latencies: list[float],
    latency_samples: list[list[float]],
    concurrency_min: int,
    concurrency_max: int,
    name: Optional[str] = None,
) -> tuple[str, str]:
    """Generate and save performance plots.

    Returns:
        Tuple of (connections/sec plot filename, latency plot filename)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_bench_{timestamp}"
    if name:
        base_name += f"_{name}"
    base_name += f"_c{concurrency_min}-{concurrency_max}"

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Plot connections per second
    ax1.plot(concurrency_levels, connections_per_sec, "b-o", linewidth=2, markersize=8)
    ax1.set_xlabel("Concurrency (workers)", fontsize=12)
    ax1.set_ylabel("Connections per Second", fontsize=12)
    ax1.set_title(
        "Connection Throughput vs Concurrency", fontsize=14, fontweight="bold"
    )
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale("log", base=2)
    ax1.set_xticks(concurrency_levels)
    ax1.set_xticklabels([str(c) for c in concurrency_levels], rotation=45)
    ax1.set_ylim(bottom=0)

    # Add value labels on the points
    for x, y in zip(concurrency_levels, connections_per_sec):
        ax1.annotate(
            f"{y:.0f}",
            (x, y),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
        )

    # Plot latency distribution as violin plot
    # Prepare data for violin plot
    positions = list(range(len(concurrency_levels)))

    # Create violin plot
    parts = ax2.violinplot(
        latency_samples,
        positions=positions,
        widths=0.7,
        showmeans=True,
        showextrema=True,
        showmedians=True,
    )

    # Customize violin plot colors
    for pc in parts["bodies"]:
        pc.set_facecolor("#ff7f0e")
        pc.set_alpha(0.7)

    ax2.set_xlabel("Concurrency (workers)", fontsize=12)
    ax2.set_ylabel("Latency (ms)", fontsize=12)
    ax2.set_title("Latency Distribution vs Concurrency", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.set_xticks(positions)
    ax2.set_xticklabels([str(c) for c in concurrency_levels], rotation=45)
    ax2.set_ylim(bottom=0)

    # Add median values as text annotations
    for i, (pos, samples) in enumerate(zip(positions, latency_samples)):
        if samples:
            median = sorted(samples)[len(samples) // 2]
            ax2.annotate(
                f"{median:.1f}",
                (pos, median),
                textcoords="offset points",
                xytext=(0, -15),
                ha="center",
                fontsize=8,
                color="darkred",
            )

    title = "PostgreSQL Connection Benchmark Results"
    if name:
        title += f": {name}"
    title += f"\n{timestamp}"

    plt.suptitle(
        title,
        fontsize=16,
        fontweight="bold",
    )
    plt.tight_layout()

    # Try to save as SVG first, fall back to PNG
    try:
        svg_filename = f"{base_name}.svg"
        plt.savefig(svg_filename, format="svg", dpi=150, bbox_inches="tight")
        plt.close()
        return svg_filename, svg_filename
    except Exception as e:
        click.echo(f"Warning: Could not save as SVG ({e}), saving as PNG instead")
        png_filename = f"{base_name}.png"
        plt.savefig(png_filename, format="png", dpi=150, bbox_inches="tight")
        plt.close()
        return png_filename, png_filename


async def run_async(
    urls: list[str],
    concurrency: Optional[int],
    concurrency_min: Optional[int],
    concurrency_max: Optional[int],
    duration: int,
    name: Optional[str] = None,
    wait_between_runs: int = 20,
) -> None:
    """Async implementation of the connection benchmark.

    Supports both single concurrency mode and range mode.
    """
    # Determine if we're in range mode or single mode
    if concurrency_min is not None and concurrency_max is not None:
        # Range mode
        click.echo("PostgreSQL Connection Throughput Benchmark - Range Mode")
        click.echo("=" * 60)
        targets = [url.split('@')[-1] if '@' in url else url for url in urls]
        if len(targets) == 1:
            click.echo(f"Target:            {targets[0]}")
        else:
            click.echo(f"Targets ({len(targets)} servers):")
            for i, target in enumerate(targets, 1):
                click.echo(f"  {i}. {target}")
        click.echo(f"Concurrency Range: {concurrency_min} - {concurrency_max} workers")
        click.echo(f"Duration per run:  {duration} seconds")
        click.echo(f"Wait between runs: {wait_between_runs} seconds")
        click.echo("=" * 60)
        click.echo()

        concurrency_levels = generate_power_of_2_range(concurrency_min, concurrency_max)
        connections_per_sec = []
        avg_latencies = []
        latency_samples = []

        click.echo(f"Testing concurrency levels: {concurrency_levels}")
        click.echo()

        for i, c in enumerate(concurrency_levels, 1):
            click.echo(f"\n{'=' * 60}")
            click.echo(f"Run {i}/{len(concurrency_levels)}: Testing with {c} workers")
            click.echo(f"{'=' * 60}\n")

            avg_rate, avg_latency, samples = await run_single_benchmark(
                urls, c, duration, quiet=False
            )
            connections_per_sec.append(avg_rate)
            avg_latencies.append(avg_latency)
            latency_samples.append(samples)

            # Brief pause between runs
            if i < len(concurrency_levels):
                click.echo(f"\nWaiting {wait_between_runs} seconds before next run...")
                await asyncio.sleep(wait_between_runs)

        # Generate summary table
        click.echo("\n" + "=" * 80)
        click.echo("OVERALL SUMMARY")
        click.echo("=" * 80)
        click.echo(f"{'Concurrency':<15} {'Conn/sec':<20} {'Avg Latency (ms)':<20}")
        click.echo("-" * 80)
        for c, rate, latency in zip(
            concurrency_levels, connections_per_sec, avg_latencies
        ):
            click.echo(f"{c:<15} {rate:<20.1f} {latency:<20.2f}")

        # Save JSON results
        click.echo("\nSaving raw results...")
        json_filename = save_json_results(
            concurrency_levels,
            connections_per_sec,
            avg_latencies,
            latency_samples,
            concurrency_min,
            concurrency_max,
            name,
            ','.join(urls),
            duration,
        )
        click.echo(f"✅ Raw results saved to: {json_filename}")

        # Generate and save plots
        click.echo("\nGenerating performance plots...")
        plot_filename, _ = save_plots(
            concurrency_levels,
            connections_per_sec,
            avg_latencies,
            latency_samples,
            concurrency_min,
            concurrency_max,
            name,
        )
        click.echo(f"✅ Performance plots saved to: {plot_filename}")

    else:
        # Single concurrency mode
        if concurrency is None:
            concurrency = 10  # Default value
        await run_single_benchmark(urls, concurrency, duration, quiet=False)


def plot_from_json(
    json_files: list[str],
    output_name: Optional[str] = None,
) -> None:
    """Load multiple JSON result files and create combined plots."""
    all_data = []

    # Load all JSON files
    for json_file in json_files:
        try:
            with open(json_file) as f:
                data = json.load(f)
                all_data.append(data)
                name = data.get("name", "unnamed")
                timestamp = data.get("timestamp", "no timestamp")
                click.echo(f"Loaded: {json_file} ({name} - {timestamp})")
        except Exception as e:
            click.echo(f"Error loading {json_file}: {e}", err=True)
            continue

    if not all_data:
        click.echo("No valid JSON files loaded.", err=True)
        return

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # Colors for different datasets
    colors = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]
    markers = ["o", "s", "^", "v", "D", "p", "*", "h", "X", "+"]

    # Plot each dataset
    for idx, data in enumerate(all_data):
        color = colors[idx % len(colors)]
        marker = markers[idx % len(markers)]

        results = data["results"]
        concurrency_levels = [r["concurrency"] for r in results]
        connections_per_sec = [r["connections_per_sec"] for r in results]
        avg_latencies = [r["avg_latency_ms"] for r in results]

        label = data.get("name", f"Run {idx + 1}")
        if data.get("timestamp"):
            timestamp = datetime.fromisoformat(data["timestamp"])
            label += f" ({timestamp.strftime('%Y-%m-%d %H:%M')})"

        # Plot connections per second
        ax1.plot(
            concurrency_levels,
            connections_per_sec,
            color=color,
            marker=marker,
            linewidth=2,
            markersize=8,
            label=label,
            alpha=0.8,
        )

        # Plot average latency
        ax2.plot(
            concurrency_levels,
            avg_latencies,
            color=color,
            marker=marker,
            linewidth=2,
            markersize=8,
            label=label,
            alpha=0.8,
        )

    # Configure connections/sec plot
    ax1.set_xlabel("Concurrency (workers)", fontsize=12)
    ax1.set_ylabel("Connections per Second", fontsize=12)
    ax1.set_title(
        "Connection Throughput vs Concurrency", fontsize=14, fontweight="bold"
    )
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale("log", base=2)
    ax1.set_ylim(bottom=0)
    ax1.legend(loc="best", fontsize=9)

    # Configure latency plot
    ax2.set_xlabel("Concurrency (workers)", fontsize=12)
    ax2.set_ylabel("Average Latency (ms)", fontsize=12)
    ax2.set_title("Average Latency vs Concurrency", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.set_xscale("log", base=2)
    ax2.set_ylim(bottom=0)
    ax2.legend(loc="best", fontsize=9)

    # Add overall title
    title = "PostgreSQL Connection Benchmark Comparison"
    if output_name:
        title += f": {output_name}"
    title += f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()

    # Save the plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_bench_comparison_{timestamp}"
    if output_name:
        base_name += f"_{output_name}"

    try:
        svg_filename = f"{base_name}.svg"
        plt.savefig(svg_filename, format="svg", dpi=150, bbox_inches="tight")
        plt.close()
        click.echo(f"\n✅ Comparison plot saved to: {svg_filename}")
    except Exception as e:
        click.echo(f"Warning: Could not save as SVG ({e}), saving as PNG instead")
        png_filename = f"{base_name}.png"
        plt.savefig(png_filename, format="png", dpi=150, bbox_inches="tight")
        plt.close()
        click.echo(f"\n✅ Comparison plot saved to: {png_filename}")


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """PostgreSQL connection throughput benchmarking tool.

    Run benchmarks or plot results from saved JSON files.
    """
    # If no subcommand is provided, show help
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.option(
    "--url",
    "-u",
    required=True,
    multiple=True,
    help="PostgreSQL connection URL (can be specified multiple times for round-robin distribution)",
    envvar="DATABASE_URL",
)
@click.option(
    "--concurrency",
    "-c",
    type=click.IntRange(min=1),
    default=None,
    help="Single concurrency level (mutually exclusive with --concurrency-min/max)",
)
@click.option(
    "--concurrency-min",
    type=click.IntRange(min=1),
    default=None,
    help="Minimum concurrency for range mode (requires --concurrency-max)",
)
@click.option(
    "--concurrency-max",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum concurrency for range mode (requires --concurrency-min)",
)
@click.option(
    "--duration",
    "-d",
    type=click.IntRange(min=1),
    default=10,
    help="Duration in seconds per benchmark run (default: 10)",
)
@click.option(
    "--name",
    "-n",
    type=str,
    default=None,
    help="Optional name for the benchmark run (included in output files and plots)",
)
@click.option(
    "--wait-between-runs",
    type=click.IntRange(min=0),
    default=20,
    help="Wait time in seconds between different concurrency runs (default: 20)",
)
def run(
    url: tuple[str, ...],
    concurrency: Optional[int],
    concurrency_min: Optional[int],
    concurrency_max: Optional[int],
    duration: int,
    name: Optional[str],
    wait_between_runs: int,
):
    """Run benchmarks on PostgreSQL database(s).

    Supports two modes:
    1. Single concurrency: Use --concurrency/-c
    2. Range mode: Use --concurrency-min and --concurrency-max to test multiple levels

    In range mode, tests powers of 2 between min and max values.
    
    When multiple URLs are provided, connections are distributed round-robin across servers.
    """
    # Validate arguments
    if concurrency is not None and (
        concurrency_min is not None or concurrency_max is not None
    ):
        raise click.UsageError(
            "Cannot use --concurrency with --concurrency-min/--concurrency-max. "
            "Choose either single mode or range mode."
        )

    if (concurrency_min is not None) != (concurrency_max is not None):
        raise click.UsageError(
            "Both --concurrency-min and --concurrency-max "
            "must be provided for range mode."
        )

    if concurrency_min is not None and concurrency_max is not None:
        if concurrency_min > concurrency_max:
            raise click.UsageError(
                "--concurrency-min must be less than or equal to --concurrency-max."
            )

    if concurrency is None and concurrency_min is None:
        # Default to single mode with concurrency=10
        concurrency = 10

    # Convert tuple to list
    urls = list(url)
    
    try:
        asyncio.run(
            run_async(
                urls,
                concurrency,
                concurrency_min,
                concurrency_max,
                duration,
                name,
                wait_between_runs,
            )
        )
    except KeyboardInterrupt:
        pass


@cli.command()
@click.argument("json_files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--output-name",
    "-o",
    type=str,
    default=None,
    help="Optional name for the output comparison plot",
)
def plot(json_files: tuple[str, ...], output_name: Optional[str]):
    """Create comparison plots from multiple benchmark JSON files.

    Example:
        pg-connection-bench plot result1.json result2.json result3.json
    """
    plot_from_json(list(json_files), output_name)


if __name__ == "__main__":
    cli()
