#!/usr/bin/env python3
"""PostgreSQL workload benchmarking tool with configurable INI workloads."""

import asyncio
import json
import math
import re
import resource
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import asyncpg
import click
import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend
import matplotlib.pyplot as plt


@dataclass
class WorkloadStats:
    """Statistics for a workload benchmark."""

    total_queries: int = 0
    failed_queries: int = 0
    latencies: list[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_report_time: float = field(default_factory=time.time)
    last_report_queries: int = 0


@dataclass
class WorkloadConfig:
    """Configuration for a single workload."""

    name: str
    queries: list[str]
    weight: float = 1.0  # Future: support weighted workloads


@dataclass
class BenchmarkConfig:
    """Complete benchmark configuration from INI file."""

    duration: int  # seconds
    concurrency_min: Optional[int] = None
    concurrency_max: Optional[int] = None
    concurrency: Optional[int] = None
    setup_queries: list[str] = field(default_factory=list)
    teardown_queries: list[str] = field(default_factory=list)
    workloads: list[WorkloadConfig] = field(default_factory=list)


def parse_duration(duration_str: str) -> int:
    """Parse duration string like '5m', '30s', '1h' into seconds."""
    duration_str = duration_str.strip().lower()

    # Check for time units
    match = re.match(r"^(\d+)([smh]?)$", duration_str)
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    value = int(match.group(1))
    unit = match.group(2) or "s"  # Default to seconds

    multipliers = {"s": 1, "m": 60, "h": 3600}

    return value * multipliers[unit]


def parse_ini_config(config_path: Path) -> BenchmarkConfig:
    """Parse INI configuration file into BenchmarkConfig."""
    # Read file manually to handle duplicate query keys
    with open(config_path) as f:
        lines = f.readlines()

    # Parse the file manually to collect duplicate query entries
    # Start with implicit global section for settings at the top of the file
    current_section = "__global__"
    sections = {"__global__": {"queries": [], "other": {}}}

    for line in lines:
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#") or line.startswith(";"):
            continue

        # Check for section header
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1]
            if current_section not in sections:
                sections[current_section] = {"queries": [], "other": {}}
        elif "=" in line:
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if key == "query":
                sections[current_section]["queries"].append(value)
            else:
                sections[current_section]["other"][key] = value

    # Parse global settings from top of file (before any section headers)
    default = sections.get("__global__", {}).get("other", {})

    # Parse duration
    duration_str = default.get("duration", "10s")
    duration = parse_duration(duration_str)

    # Parse concurrency settings
    concurrency = None
    concurrency_min = None
    concurrency_max = None

    if "concurrency" in default:
        concurrency = int(default["concurrency"])
    if "concurrency-min" in default:
        concurrency_min = int(default["concurrency-min"])
    if "concurrency-max" in default:
        concurrency_max = int(default["concurrency-max"])

    # Initialize config object
    benchmark_config = BenchmarkConfig(
        duration=duration,
        concurrency=concurrency,
        concurrency_min=concurrency_min,
        concurrency_max=concurrency_max,
    )

    # Parse sections
    for section_name, section_data in sections.items():
        if section_name == "__global__":
            continue

        queries = section_data["queries"]

        if section_name.lower() == "setup":
            benchmark_config.setup_queries = queries
        elif section_name.lower() == "teardown":
            benchmark_config.teardown_queries = queries
        else:
            # This is a workload section
            if queries:
                workload = WorkloadConfig(name=section_name, queries=queries)
                benchmark_config.workloads.append(workload)

    # Validate configuration
    if not benchmark_config.workloads:
        raise ValueError("No workload sections found in configuration")

    return benchmark_config


class WorkloadBenchmark:
    """Benchmarks PostgreSQL workload throughput."""

    def __init__(self, postgres_url: str, concurrency: int, workload: WorkloadConfig):
        self.postgres_url = postgres_url
        self.concurrency = concurrency
        self.workload = workload
        self.shutdown = False
        self.stats = WorkloadStats()
        self.stats_lock = asyncio.Lock()
        self.connections: list[asyncpg.Connection] = []

    async def setup_connections(self) -> None:
        """Create and store persistent connections."""
        for i in range(self.concurrency):
            try:
                conn = await asyncpg.connect(self.postgres_url, timeout=30)
                self.connections.append(conn)
            except Exception as e:
                click.echo(f"Failed to create connection {i + 1}: {e}", err=True)
                raise

    async def cleanup_connections(self) -> None:
        """Close all connections."""
        for conn in self.connections:
            try:
                await conn.close()
            except Exception:
                pass
        self.connections.clear()

    async def worker(self, worker_id: int, connection: asyncpg.Connection) -> None:
        """Worker that continuously executes workload queries."""
        while not self.shutdown:
            try:
                # Execute all queries in the workload
                start = time.perf_counter()

                for query in self.workload.queries:
                    await connection.execute(query)

                latency = (time.perf_counter() - start) * 1000

                async with self.stats_lock:
                    self.stats.total_queries += len(self.workload.queries)
                    self.stats.latencies.append(latency)

            except asyncio.CancelledError:
                break
            except Exception:
                async with self.stats_lock:
                    self.stats.failed_queries += len(self.workload.queries)
                if not self.shutdown:
                    await asyncio.sleep(0.01)

    async def reporter(self) -> None:
        """Reports statistics every second."""
        await asyncio.sleep(1)

        while not self.shutdown:
            async with self.stats_lock:
                current_time = time.time()
                time_delta = current_time - self.stats.last_report_time
                query_delta = self.stats.total_queries - self.stats.last_report_queries

                if time_delta > 0:
                    rate = query_delta / time_delta

                    if self.stats.latencies:
                        recent_latencies = self.stats.latencies[
                            -min(1000, len(self.stats.latencies)) :
                        ]
                        avg_latency = sum(recent_latencies) / len(recent_latencies)
                        min_latency = min(recent_latencies)
                        max_latency = max(recent_latencies)

                        click.echo(
                            f"[{int(current_time - self.stats.start_time):>4}s] "
                            f"Workload: {self.workload.name:<20} | "
                            f"Rate: {rate:>7.1f} q/s | "
                            f"Total: {self.stats.total_queries:>7} | "
                            f"Failed: {self.stats.failed_queries:>5} | "
                            f"Latency (ms): avg={avg_latency:>6.1f} "
                            f"min={min_latency:>6.1f} max={max_latency:>6.1f}"
                        )
                    else:
                        click.echo(
                            f"[{int(current_time - self.stats.start_time):>4}s] "
                            f"Workload: {self.workload.name:<20} | "
                            f"Rate: {rate:>7.1f} q/s | "
                            f"Total: {self.stats.total_queries:>7} | "
                            f"Failed: {self.stats.failed_queries:>5}"
                        )

                self.stats.last_report_time = current_time
                self.stats.last_report_queries = self.stats.total_queries

            await asyncio.sleep(1)

    def print_histogram(self) -> None:
        """Print a histogram of workload latencies."""
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
        click.echo(f"Workload Summary: {self.workload.name}")
        click.echo("=" * 60)

        click.echo(f"Duration:             {duration:.1f} seconds")
        click.echo(f"Concurrency:          {self.concurrency} workers")
        click.echo(f"Total Queries:        {self.stats.total_queries}")
        click.echo(f"Failed Queries:       {self.stats.failed_queries}")

        if self.stats.total_queries > 0:
            success_rate = (
                (self.stats.total_queries - self.stats.failed_queries)
                / self.stats.total_queries
                * 100
            )
            click.echo(f"Success Rate:         {success_rate:.1f}%")

        if duration > 0:
            avg_rate = self.stats.total_queries / duration
            click.echo(f"Average Rate:         {avg_rate:.1f} queries/second")

        if self.stats.latencies:
            avg_latency = sum(self.stats.latencies) / len(self.stats.latencies)
            min_latency = min(self.stats.latencies)
            max_latency = max(self.stats.latencies)

            click.echo("\nLatency Statistics:")
            click.echo(f"  Average:            {avg_latency:.2f} ms")
            click.echo(f"  Minimum:            {min_latency:.2f} ms")
            click.echo(f"  Maximum:            {max_latency:.2f} ms")

            self.print_histogram()


async def run_setup(postgres_url: str, setup_queries: list[str]) -> None:
    """Run setup queries before benchmark."""
    if not setup_queries:
        return

    click.echo("\nRunning setup queries...")
    conn = await asyncpg.connect(postgres_url, timeout=30)
    try:
        for query in setup_queries:
            click.echo(f"  Executing: {query[:50]}...")
            await conn.execute(query)
        click.echo("Setup completed successfully.\n")
    finally:
        await conn.close()


async def run_teardown(postgres_url: str, teardown_queries: list[str]) -> None:
    """Run teardown queries after benchmark."""
    if not teardown_queries:
        return

    click.echo("\nRunning teardown queries...")
    conn = await asyncpg.connect(postgres_url, timeout=30)
    try:
        for query in teardown_queries:
            click.echo(f"  Executing: {query[:50]}...")
            await conn.execute(query)
        click.echo("Teardown completed successfully.")
    finally:
        await conn.close()


async def run_single_workload_benchmark(
    url: str,
    concurrency: int,
    duration: int,
    workload: WorkloadConfig,
    quiet: bool,
) -> tuple[str, float, float, list[float]]:
    """Run a single workload benchmark at a specific concurrency level.

    Returns:
        Tuple of (workload_name, average queries per second, average latency in ms, latency samples)
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
        click.echo(f"\nBenchmarking Workload: {workload.name}")
        click.echo("-" * 60)
        click.echo(f"Queries in workload: {len(workload.queries)}")
        click.echo(f"Concurrency:         {concurrency} workers")
        click.echo(f"Duration:            {duration} seconds")
        click.echo("-" * 60)
        click.echo()

    benchmark = WorkloadBenchmark(url, concurrency, workload)

    # Setup connections
    await benchmark.setup_connections()

    loop = asyncio.get_event_loop()

    def signal_handler(sig):
        click.echo("\n\nReceived interrupt signal, shutting down...")
        benchmark.shutdown = True
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    try:
        workers = [
            benchmark.worker(i, benchmark.connections[i]) for i in range(concurrency)
        ]
        reporter = benchmark.reporter()

        # Create tasks
        worker_tasks = [asyncio.create_task(w) for w in workers]
        reporter_task = asyncio.create_task(reporter)

        # Wait for the duration
        await asyncio.sleep(duration)

        # Signal shutdown
        benchmark.shutdown = True

        # Cancel all tasks
        for task in worker_tasks + [reporter_task]:
            task.cancel()

        # Wait for tasks to finish
        await asyncio.gather(*worker_tasks, reporter_task, return_exceptions=True)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        benchmark.shutdown = True
        await benchmark.cleanup_connections()
        await asyncio.sleep(0.1)
        if not quiet:
            benchmark.print_summary()

    # Calculate metrics
    duration = time.time() - benchmark.stats.start_time
    avg_rate = benchmark.stats.total_queries / duration if duration > 0 else 0
    avg_latency = (
        sum(benchmark.stats.latencies) / len(benchmark.stats.latencies)
        if benchmark.stats.latencies
        else 0
    )
    # Sample latencies for visualization (up to 10000 samples)
    latency_samples = benchmark.stats.latencies[:10000] if len(benchmark.stats.latencies) > 10000 else benchmark.stats.latencies.copy()
    return workload.name, avg_rate, avg_latency, latency_samples


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

    # Include endpoints if not powers of 2
    if min_val not in values and min_val > 0:
        values.insert(0, min_val)

    if max_val not in values and max_val > min_val:
        values.append(max_val)

    return sorted(values)


def save_json_results(
    workload_results: dict,
    workload_path: str,
    benchmark_config: BenchmarkConfig,
    name: Optional[str] = None,
    url: Optional[str] = None,
) -> str:
    """Save benchmark results as JSON.

    Returns:
        JSON filename
    """
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_workload_bench_{timestamp_str}"
    if name:
        base_name += f"_{name}"
    if benchmark_config.concurrency_min and benchmark_config.concurrency_max:
        base_name += (
            f"_c{benchmark_config.concurrency_min}-{benchmark_config.concurrency_max}"
        )
    json_filename = f"{base_name}.json"

    results = {
        "timestamp": timestamp.isoformat(),
        "name": name,
        "workload_file": workload_path,
        "url": url.split("@")[-1] if url and "@" in url else url,
        "duration_per_run": benchmark_config.duration,
        "workloads": {},
    }

    if benchmark_config.concurrency_min and benchmark_config.concurrency_max:
        results["concurrency_min"] = benchmark_config.concurrency_min
        results["concurrency_max"] = benchmark_config.concurrency_max
    elif benchmark_config.concurrency:
        results["concurrency"] = benchmark_config.concurrency

    # Organize results by workload
    for workload_name, data in workload_results.items():
        # Add percentiles to each result if we have latency samples
        enhanced_results = []
        for r in data["results"]:
            result = {
                "concurrency": r["concurrency"],
                "queries_per_sec": r["queries_per_sec"],
                "avg_latency_ms": r["avg_latency_ms"]
            }
            if "latency_samples" in r and r["latency_samples"]:
                samples = r["latency_samples"]
                sorted_samples = sorted(samples)
                result["latency_percentiles"] = {
                    "p50": float(sorted_samples[len(sorted_samples)//2]),
                    "p95": float(sorted_samples[int(len(sorted_samples)*0.95)]),
                    "p99": float(sorted_samples[int(len(sorted_samples)*0.99)])
                }
            enhanced_results.append(result)
        results["workloads"][workload_name] = {"results": enhanced_results}

    with open(json_filename, "w") as f:
        json.dump(results, f, indent=2)

    return json_filename


def save_plots(
    workload_results: dict,
    benchmark_config: BenchmarkConfig,
    name: Optional[str] = None,
) -> str:
    """Generate and save performance plots for all workloads.

    Returns:
        Plot filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_workload_bench_{timestamp}"
    if name:
        base_name += f"_{name}"
    if benchmark_config.concurrency_min and benchmark_config.concurrency_max:
        base_name += (
            f"_c{benchmark_config.concurrency_min}-{benchmark_config.concurrency_max}"
        )

    # Create figure with subplots for each workload (side-by-side layout)
    num_workloads = len(workload_results)
    fig, axes = plt.subplots(num_workloads, 2, figsize=(14, 6 * num_workloads))

    # Handle case of single workload
    if num_workloads == 1:
        axes = axes.reshape(1, 2)

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

    for idx, (workload_name, data) in enumerate(workload_results.items()):
        results = data["results"]
        concurrency_levels = [r["concurrency"] for r in results]
        queries_per_sec = [r["queries_per_sec"] for r in results]
        avg_latencies = [r["avg_latency_ms"] for r in results]
        latency_samples = [r.get("latency_samples", []) for r in results]

        color = colors[idx % len(colors)]

        # Plot queries per second (left column)
        ax1 = axes[idx, 0]
        ax1.plot(
            concurrency_levels,
            queries_per_sec,
            color=color,
            marker="o",
            linewidth=2,
            markersize=8,
        )
        ax1.set_xlabel("Concurrency (workers)", fontsize=11)
        ax1.set_ylabel("Queries per Second", fontsize=11)
        ax1.set_title(f"Throughput: {workload_name}", fontsize=12, fontweight="bold")
        ax1.grid(True, alpha=0.3)
        ax1.set_xscale("log", base=2)
        ax1.set_xticks(concurrency_levels)
        ax1.set_xticklabels([str(c) for c in concurrency_levels], rotation=45)
        ax1.set_ylim(bottom=0)

        # Add value labels
        for x, y in zip(concurrency_levels, queries_per_sec):
            ax1.annotate(
                f"{y:.0f}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=8,
            )

        # Plot latency distribution as violin plot (right column)
        ax2 = axes[idx, 1]
        
        # Prepare data for violin plot
        positions = list(range(len(concurrency_levels)))
        
        # Filter out empty sample lists
        valid_samples = [s if s else [0] for s in latency_samples]
        
        # Create violin plot
        if any(len(s) > 0 for s in valid_samples):
            parts = ax2.violinplot(
                valid_samples,
                positions=positions,
                widths=0.7,
                showmeans=True,
                showextrema=True,
                showmedians=True
            )
            
            # Customize violin plot colors
            for pc in parts['bodies']:
                pc.set_facecolor(color)
                pc.set_alpha(0.7)
        
        ax2.set_xlabel("Concurrency (workers)", fontsize=11)
        ax2.set_ylabel("Latency (ms)", fontsize=11)
        ax2.set_title(f"Latency Distribution: {workload_name}", fontsize=12, fontweight="bold")
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.set_xticks(positions)
        ax2.set_xticklabels([str(c) for c in concurrency_levels], rotation=45)
        ax2.set_ylim(bottom=0)
        
        # Add median values as text annotations
        for i, (pos, samples) in enumerate(zip(positions, valid_samples)):
            if samples and len(samples) > 0:
                median = sorted(samples)[len(samples)//2]
                ax2.annotate(
                    f"{median:.1f}",
                    (pos, median),
                    textcoords="offset points",
                    xytext=(0, -15),
                    ha="center",
                    fontsize=8,
                    color='darkred'
                )

    title = "PostgreSQL Workload Benchmark Results"
    if name:
        title += f": {name}"
    title += f"\n{timestamp}"

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()

    # Save plot
    try:
        svg_filename = f"{base_name}.svg"
        plt.savefig(svg_filename, format="svg", dpi=150, bbox_inches="tight")
        plt.close()
        return svg_filename
    except Exception as e:
        click.echo(f"Warning: Could not save as SVG ({e}), saving as PNG instead")
        png_filename = f"{base_name}.png"
        plt.savefig(png_filename, format="png", dpi=150, bbox_inches="tight")
        plt.close()
        return png_filename


async def run_async(
    url: str,
    workload_path: str,
    name: Optional[str],
    wait_between_runs: int,
) -> None:
    """Async implementation of the workload benchmark."""
    # Parse workload configuration
    workload_file = Path(workload_path)
    if not workload_file.exists():
        raise click.ClickException(f"Workload file not found: {workload_path}")

    try:
        benchmark_config = parse_ini_config(workload_file)
    except Exception as e:
        raise click.ClickException(f"Error parsing workload: {e}") from e

    # Run setup queries
    await run_setup(url, benchmark_config.setup_queries)

    try:
        # Determine if we're in range mode or single mode
        if benchmark_config.concurrency_min and benchmark_config.concurrency_max:
            # Range mode
            click.echo("PostgreSQL Workload Benchmark - Range Mode")
            click.echo("=" * 60)
            click.echo(f"Workload:          {workload_path}")
            click.echo(
                f"Target:            {url.split('@')[-1] if '@' in url else url}"
            )
            workload_names = ", ".join(w.name for w in benchmark_config.workloads)
            click.echo(f"Workloads:         {workload_names}")
            click.echo(
                f"Concurrency Range: {benchmark_config.concurrency_min} - "
                f"{benchmark_config.concurrency_max} workers"
            )
            click.echo(f"Duration per run:  {benchmark_config.duration} seconds")
            click.echo(f"Wait between runs: {wait_between_runs} seconds")
            click.echo("=" * 60)

            concurrency_levels = generate_power_of_2_range(
                benchmark_config.concurrency_min, benchmark_config.concurrency_max
            )

            workload_results = {
                w.name: {"results": []} for w in benchmark_config.workloads
            }

            click.echo(f"\nTesting concurrency levels: {concurrency_levels}")

            for i, c in enumerate(concurrency_levels, 1):
                click.echo(f"\n{'=' * 60}")
                click.echo(
                    f"Run {i}/{len(concurrency_levels)}: Testing with {c} workers"
                )
                click.echo(f"{'=' * 60}")

                for workload in benchmark_config.workloads:
                    (
                        workload_name,
                        avg_rate,
                        avg_latency,
                        samples,
                    ) = await run_single_workload_benchmark(
                        url, c, benchmark_config.duration, workload, quiet=False
                    )

                    workload_results[workload_name]["results"].append(
                        {
                            "concurrency": c,
                            "queries_per_sec": avg_rate,
                            "avg_latency_ms": avg_latency,
                            "latency_samples": samples,
                        }
                    )

                    # Brief pause between workloads
                    if workload != benchmark_config.workloads[-1]:
                        await asyncio.sleep(1)

                # Brief pause between concurrency levels
                if i < len(concurrency_levels):
                    click.echo(f"\nWaiting {wait_between_runs} seconds before next run...")
                    await asyncio.sleep(wait_between_runs)

            # Generate summary table
            click.echo("\n" + "=" * 80)
            click.echo("OVERALL SUMMARY")
            click.echo("=" * 80)

            for workload_name, data in workload_results.items():
                click.echo(f"\nWorkload: {workload_name}")
                click.echo("-" * 80)
                click.echo(
                    f"{'Concurrency':<15} {'Queries/sec':<20} {'Avg Latency (ms)':<20}"
                )
                click.echo("-" * 80)
                for result in data["results"]:
                    click.echo(
                        f"{result['concurrency']:<15} "
                        f"{result['queries_per_sec']:<20.1f} "
                        f"{result['avg_latency_ms']:<20.2f}"
                    )

            # Save results
            click.echo("\nSaving results...")
            json_filename = save_json_results(
                workload_results, workload_path, benchmark_config, name, url
            )
            click.echo(f"✅ Raw results saved to: {json_filename}")

            plot_filename = save_plots(workload_results, benchmark_config, name)
            click.echo(f"✅ Performance plots saved to: {plot_filename}")

        else:
            # Single concurrency mode
            concurrency = benchmark_config.concurrency or 10

            click.echo("PostgreSQL Workload Benchmark - Single Mode")
            click.echo("=" * 60)
            click.echo(f"Workload:    {workload_path}")
            click.echo(f"Target:      {url.split('@')[-1] if '@' in url else url}")
            click.echo(
                f"Workloads:   {', '.join(w.name for w in benchmark_config.workloads)}"
            )
            click.echo(f"Concurrency: {concurrency} workers")
            click.echo(f"Duration:    {benchmark_config.duration} seconds")
            click.echo("=" * 60)

            workload_results = {}
            for workload in benchmark_config.workloads:
                (
                    workload_name,
                    avg_rate,
                    avg_latency,
                    samples,
                ) = await run_single_workload_benchmark(
                    url, concurrency, benchmark_config.duration, workload, quiet=False
                )
                workload_results[workload_name] = {
                    "results": [
                        {
                            "concurrency": concurrency,
                            "queries_per_sec": avg_rate,
                            "avg_latency_ms": avg_latency,
                            "latency_samples": samples,
                        }
                    ]
                }

            # Save results
            json_filename = save_json_results(
                workload_results, workload_path, benchmark_config, name, url
            )
            click.echo(f"\n✅ Raw results saved to: {json_filename}")

    finally:
        # Run teardown queries
        await run_teardown(url, benchmark_config.teardown_queries)


def plot_from_json(
    json_files: list[str],
    output_name: Optional[str],
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

    # Get all unique workload names
    all_workloads = set()
    for data in all_data:
        all_workloads.update(data.get("workloads", {}).keys())

    workloads_list = sorted(all_workloads)
    num_workloads = len(workloads_list)

    if num_workloads == 0:
        click.echo("No workloads found in JSON files.", err=True)
        return

    # Create figure (side-by-side layout)
    fig, axes = plt.subplots(num_workloads, 2, figsize=(14, 6 * num_workloads))

    # Handle single workload case
    if num_workloads == 1:
        axes = axes.reshape(1, 2)

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

    for workload_idx, workload_name in enumerate(workloads_list):
        ax1 = axes[workload_idx, 0]
        ax2 = axes[workload_idx, 1]

        for data_idx, data in enumerate(all_data):
            if workload_name not in data.get("workloads", {}):
                continue

            color = colors[data_idx % len(colors)]
            marker = markers[data_idx % len(markers)]

            results = data["workloads"][workload_name]["results"]
            concurrency_levels = [r["concurrency"] for r in results]
            queries_per_sec = [r["queries_per_sec"] for r in results]
            avg_latencies = [r["avg_latency_ms"] for r in results]

            label = data.get("name", f"Run {data_idx + 1}")
            if data.get("timestamp"):
                timestamp = datetime.fromisoformat(data["timestamp"])
                label += f" ({timestamp.strftime('%Y-%m-%d %H:%M')})"

            # Plot throughput
            ax1.plot(
                concurrency_levels,
                queries_per_sec,
                color=color,
                marker=marker,
                linewidth=2,
                markersize=8,
                label=label,
                alpha=0.8,
            )

            # Plot latency
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

        # Configure throughput subplot
        ax1.set_xlabel("Concurrency (workers)", fontsize=11)
        ax1.set_ylabel("Queries per Second", fontsize=11)
        ax1.set_title(f"Throughput: {workload_name}", fontsize=12, fontweight="bold")
        ax1.grid(True, alpha=0.3)
        ax1.set_xscale("log", base=2)
        ax1.set_ylim(bottom=0)
        ax1.legend(loc="best", fontsize=8)

        # Configure latency subplot
        ax2.set_xlabel("Concurrency (workers)", fontsize=11)
        ax2.set_ylabel("Average Latency (ms)", fontsize=11)
        ax2.set_title(f"Latency: {workload_name}", fontsize=12, fontweight="bold")
        ax2.grid(True, alpha=0.3)
        ax2.set_xscale("log", base=2)
        ax2.set_ylim(bottom=0)
        ax2.legend(loc="best", fontsize=8)

    # Add overall title
    title = "PostgreSQL Workload Benchmark Comparison"
    if output_name:
        title += f": {output_name}"
    title += f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()

    # Save plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_workload_bench_comparison_{timestamp}"
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
    """PostgreSQL workload benchmarking tool with INI configuration.

    Run benchmarks using workload configurations or plot results from saved JSON files.
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.option(
    "--url",
    "-u",
    required=True,
    help="PostgreSQL connection URL (e.g., postgresql://user:pass@localhost/dbname)",
    envvar="DATABASE_URL",
)
@click.option(
    "--workload",
    "-w",
    required=True,
    type=click.Path(exists=True),
    help="Path to INI workload file",
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
def run(url: str, workload: str, name: Optional[str], wait_between_runs: int):
    """Run workload benchmarks using an INI configuration file.

    The INI file should define workloads, setup/teardown queries, and parameters.
    Global settings are placed at the top of the file without a section header.

    Example INI file:

    \b
    # Global settings at the top (no section header needed)
    duration=5m
    concurrency-min=1
    concurrency-max=256

    [setup]
    query=drop table if exists t
    query=create table t(a int)

    [teardown]
    query=drop table t

    [select 1]
    query=select 1

    [insert workload]
    query=insert into t values (1)
    query=select count(*) from t
    """
    try:
        asyncio.run(run_async(url, workload, name, wait_between_runs))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        raise click.ClickException(str(e)) from e


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
        pg-workload-bench plot result1.json result2.json result3.json
    """
    plot_from_json(list(json_files), output_name)


if __name__ == "__main__":
    cli()
