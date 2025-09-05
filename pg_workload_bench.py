#!/usr/bin/env python3
"""PostgreSQL workload benchmarking tool with configurable INI workloads."""

import asyncio
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import asyncpg
import click

from pg_bench_common.database import (
    format_target_list,
    run_queries_on_connection,
    setup_database_connection,
)
from pg_bench_common.plotting import (
    create_comparison_violin_plots,
    create_violin_plots,
    enhance_results_with_percentiles,
    load_json_results,
    save_json_results,
)
from pg_bench_common.reporting import (
    WorkloadReporter,
    print_benchmark_summary,
    print_summary_header,
)
from pg_bench_common.statistics import BenchmarkStats, print_latency_stats
from pg_bench_common.system import adjust_file_descriptor_limits, setup_signal_handlers
from pg_bench_common.utils import (
    generate_power_of_2_range,
    parse_duration,
    sample_latencies,
)


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
    wait_between_runs: int = 20  # seconds between runs in range mode
    setup_queries: list[str] = field(default_factory=list)
    teardown_queries: list[str] = field(default_factory=list)
    workloads: list[WorkloadConfig] = field(default_factory=list)


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

    # Parse wait-between-runs setting
    wait_between_runs = 20  # default value in seconds
    if "wait-between-runs" in default:
        wait_between_runs_str = default["wait-between-runs"]
        # Try to parse as duration string (e.g., "10s", "2m"), fall back to int
        try:
            wait_between_runs = parse_duration(wait_between_runs_str)
        except (ValueError, AttributeError):
            wait_between_runs = int(wait_between_runs_str)

    # Initialize config object
    benchmark_config = BenchmarkConfig(
        duration=duration,
        concurrency=concurrency,
        concurrency_min=concurrency_min,
        concurrency_max=concurrency_max,
        wait_between_runs=wait_between_runs,
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

    def __init__(self, postgres_urls: list[str], concurrency: int, workload: WorkloadConfig):
        self.postgres_urls = postgres_urls
        self.concurrency = concurrency
        self.workload = workload
        self.shutdown = False
        self.stats = BenchmarkStats()
        self.stats_lock = asyncio.Lock()
        self.connections: list[asyncpg.Connection] = []
        self.actual_end_time: Optional[float] = None

    async def setup_connections(self) -> None:
        """Create and store persistent connections."""
        for i in range(self.concurrency):
            try:
                # Round-robin distribution across URLs
                url_index = i % len(self.postgres_urls)
                postgres_url = self.postgres_urls[url_index]
                conn = await setup_database_connection(postgres_url)
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
                    self.stats.total_operations += len(self.workload.queries)
                    self.stats.latencies.append(latency)

            except asyncio.CancelledError:
                break
            except Exception:
                async with self.stats_lock:
                    self.stats.failed_operations += len(self.workload.queries)
                if not self.shutdown:
                    await asyncio.sleep(0.01)

    def print_summary(self) -> None:
        """Print final summary statistics."""
        duration = (
            self.actual_end_time - self.stats.start_time
            if self.actual_end_time
            else time.time() - self.stats.start_time
        )

        print_summary_header(f"Workload Summary: {self.workload.name}")
        print_benchmark_summary(
            duration,
            self.concurrency,
            self.stats.total_operations,
            self.stats.failed_operations,
            self.stats.latencies,
            "queries",
        )

        if self.stats.latencies:
            print_latency_stats(self.stats.latencies)


async def run_single_workload_benchmark(
    urls: list[str],
    concurrency: int,
    duration: int,
    workload: WorkloadConfig,
    quiet: bool,
) -> tuple[str, float, float, list[float]]:
    """Run a single workload benchmark at a specific concurrency level.

    Returns:
        Tuple of (workload_name, average queries per second, average latency in ms, latency samples)
    """
    needed_fds = concurrency * 4 + 1000
    adjust_file_descriptor_limits(needed_fds, quiet)

    benchmark = None
    reporter = None
    cancelled = False

    try:
        if not quiet:
            click.echo(f"\nBenchmarking Workload: {workload.name}")
            click.echo("-" * 60)
            click.echo(f"Queries in workload: {len(workload.queries)}")
            click.echo(f"Concurrency:         {concurrency} workers")
            click.echo(f"Duration:            {duration} seconds")
            click.echo("-" * 60)
            click.echo()

        benchmark = WorkloadBenchmark(urls, concurrency, workload)

        # Setup connections
        await benchmark.setup_connections()

        # Reset start time after connections are established to exclude setup time from QPS calculation
        benchmark.stats.start_time = time.time()
        benchmark.stats.last_report_time = time.time()

        reporter = WorkloadReporter(benchmark.stats, benchmark.stats_lock, workload.name)

        def shutdown_handler(sig):
            click.echo("\n\nReceived interrupt signal, shutting down...")
            benchmark.shutdown = True
            reporter.stop()
            loop = asyncio.get_event_loop()
            for task in asyncio.all_tasks(loop):
                task.cancel()

        setup_signal_handlers(shutdown_handler)

        workers = [benchmark.worker(i, benchmark.connections[i]) for i in range(concurrency)]
        reporter_task = asyncio.create_task(reporter.run())

        # Create tasks
        worker_tasks = [asyncio.create_task(w) for w in workers]

        # Wait for the duration
        await asyncio.sleep(duration)

        # Signal shutdown - workers will finish their current batch then stop
        benchmark.shutdown = True
        reporter.stop()

        # Wait for workers to complete naturally (finish in-flight queries)
        await asyncio.gather(*worker_tasks, return_exceptions=False)

        # Record when all workers have actually finished
        benchmark.actual_end_time = time.time()

        # Cancel the reporter task
        reporter_task.cancel()
        await asyncio.gather(reporter_task, return_exceptions=True)

    except (asyncio.CancelledError, KeyboardInterrupt):
        # Mark that we were cancelled but still need to cleanup
        cancelled = True
    finally:
        if benchmark:
            benchmark.shutdown = True
            await benchmark.cleanup_connections()
            await asyncio.sleep(0.1)
            if not quiet and not cancelled:
                benchmark.print_summary()

        # Re-raise the cancellation after cleanup
        if cancelled:
            raise asyncio.CancelledError()

    # Calculate metrics using actual completion time
    actual_duration = (
        benchmark.actual_end_time - benchmark.stats.start_time
        if benchmark.actual_end_time
        else time.time() - benchmark.stats.start_time
    )
    avg_rate = benchmark.stats.total_operations / actual_duration if actual_duration > 0 else 0
    avg_latency = sum(benchmark.stats.latencies) / len(benchmark.stats.latencies) if benchmark.stats.latencies else 0
    latency_samples = sample_latencies(benchmark.stats.latencies)
    return workload.name, avg_rate, avg_latency, latency_samples


async def run_async(
    urls: list[str],
    workload_path: str,
    name: Optional[str],
    cli_duration: Optional[str],
    cli_concurrency: Optional[int],
    cli_concurrency_min: Optional[int],
    cli_concurrency_max: Optional[int],
    cli_wait_between_runs: Optional[int],
    plot_format: str,
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

    # Apply CLI overrides if provided
    if cli_duration is not None:
        benchmark_config.duration = parse_duration(cli_duration)

    if cli_concurrency is not None:
        # Setting concurrency overrides range mode
        benchmark_config.concurrency = cli_concurrency
        benchmark_config.concurrency_min = None
        benchmark_config.concurrency_max = None

    if cli_concurrency_min is not None:
        benchmark_config.concurrency_min = cli_concurrency_min

    if cli_concurrency_max is not None:
        benchmark_config.concurrency_max = cli_concurrency_max

    if cli_wait_between_runs is not None:
        benchmark_config.wait_between_runs = cli_wait_between_runs

    # Run setup queries
    await run_queries_on_connection(urls, benchmark_config.setup_queries, "setup queries")

    try:
        # Determine if we're in range mode or single mode
        if benchmark_config.concurrency_min and benchmark_config.concurrency_max:
            # Range mode
            click.echo("PostgreSQL Workload Benchmark - Range Mode")
            click.echo("=" * 60)
            click.echo(f"Workload:          {workload_path}")
            targets = format_target_list(urls)
            if len(targets) == 1:
                click.echo(f"Target:            {targets[0]}")
            else:
                click.echo(f"Targets ({len(targets)} servers):")
                for i, target in enumerate(targets, 1):
                    click.echo(f"  {i}. {target}")
            workload_names = ", ".join(w.name for w in benchmark_config.workloads)
            click.echo(f"Workloads:         {workload_names}")
            click.echo(
                f"Concurrency Range: {benchmark_config.concurrency_min} - {benchmark_config.concurrency_max} workers"
            )
            click.echo(f"Duration per run:  {benchmark_config.duration} seconds")
            click.echo(f"Wait between runs: {benchmark_config.wait_between_runs} seconds")
            click.echo("=" * 60)

            concurrency_levels = generate_power_of_2_range(
                benchmark_config.concurrency_min, benchmark_config.concurrency_max
            )

            workload_results = {w.name: {"results": []} for w in benchmark_config.workloads}

            click.echo(f"\nTesting concurrency levels: {concurrency_levels}")

            for i, c in enumerate(concurrency_levels, 1):
                click.echo(f"\n{'=' * 60}")
                click.echo(f"Run {i}/{len(concurrency_levels)}: Testing with {c} workers")
                click.echo(f"{'=' * 60}")

                for workload in benchmark_config.workloads:
                    (
                        workload_name,
                        avg_rate,
                        avg_latency,
                        samples,
                    ) = await run_single_workload_benchmark(urls, c, benchmark_config.duration, workload, quiet=False)

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
                    click.echo(f"\nWaiting {benchmark_config.wait_between_runs} seconds before next run...")
                    await asyncio.sleep(benchmark_config.wait_between_runs)

            # Generate summary table
            click.echo("\n" + "=" * 80)
            click.echo("OVERALL SUMMARY")
            click.echo("=" * 80)

            for workload_name, data in workload_results.items():
                click.echo(f"\nWorkload: {workload_name}")
                click.echo("-" * 80)
                click.echo(f"{'Concurrency':<15} {'Queries/sec':<20} {'Avg Latency (ms)':<20}")
                click.echo("-" * 80)
                for result in data["results"]:
                    click.echo(
                        f"{result['concurrency']:<15} "
                        f"{result['queries_per_sec']:<20.1f} "
                        f"{result['avg_latency_ms']:<20.2f}"
                    )

            # Save results
            click.echo("\nSaving results...")

            # Transform results for JSON output
            results_data = {
                "workload_file": workload_path,
                "duration_per_run": benchmark_config.duration,
                "concurrency_min": benchmark_config.concurrency_min,
                "concurrency_max": benchmark_config.concurrency_max,
                "workloads": {},
            }

            for workload_name, data in workload_results.items():
                results_data["workloads"][workload_name] = {
                    "results": enhance_results_with_percentiles(data["results"])
                }

            json_filename = save_json_results(
                results_data,
                "pg_workload_bench",
                name,
                ",".join(urls),
                benchmark_config.concurrency_min,
                benchmark_config.concurrency_max,
            )
            click.echo(f"✅ Raw results saved to: {json_filename}")

            # Generate plots
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"pg_workload_bench_{timestamp}"
            if name:
                base_name += f"_{name}"
            base_name += f"_c{benchmark_config.concurrency_min}-{benchmark_config.concurrency_max}"

            # Prepare datasets for plotting
            datasets = []
            for workload_name, data in workload_results.items():
                results = data["results"]
                datasets.append(
                    {
                        "label": workload_name,
                        "concurrency_levels": [r["concurrency"] for r in results],
                        "throughput": [r["queries_per_sec"] for r in results],
                        "avg_latencies": [r["avg_latency_ms"] for r in results],
                        "latency_samples": [r.get("latency_samples", []) for r in results],
                    }
                )

            title = "PostgreSQL Workload Benchmark Results"
            if name:
                title += f": {name}"
            title += f"\n{timestamp}"

            plot_filename = create_violin_plots(title, datasets, base_name, plot_format)
            click.echo(f"✅ Performance plots saved to: {plot_filename}")

        else:
            # Single concurrency mode
            concurrency = benchmark_config.concurrency or 10

            click.echo("PostgreSQL Workload Benchmark - Single Mode")
            click.echo("=" * 60)
            click.echo(f"Workload:    {workload_path}")
            targets = format_target_list(urls)
            if len(targets) == 1:
                click.echo(f"Target:      {targets[0]}")
            else:
                click.echo(f"Targets ({len(targets)} servers):")
                for i, target in enumerate(targets, 1):
                    click.echo(f"  {i}. {target}")
            click.echo(f"Workloads:   {', '.join(w.name for w in benchmark_config.workloads)}")
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
                    urls, concurrency, benchmark_config.duration, workload, quiet=False
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
            results_data = {
                "workload_file": workload_path,
                "duration_per_run": benchmark_config.duration,
                "concurrency": concurrency,
                "workloads": {
                    workload_name: {"results": enhance_results_with_percentiles(data["results"])}
                    for workload_name, data in workload_results.items()
                },
            }
            json_filename = save_json_results(results_data, "pg_workload_bench", name, ",".join(urls))
            click.echo(f"\n✅ Raw results saved to: {json_filename}")

    finally:
        # Run teardown queries
        await run_queries_on_connection(urls, benchmark_config.teardown_queries, "teardown queries")


def plot_from_json(
    json_files: list[str],
    output_name: Optional[str],
    plot_format: str,
) -> None:
    """Load multiple JSON result files and create combined plots."""
    try:
        all_data = load_json_results(json_files)
    except ValueError as e:
        click.echo(str(e), err=True)
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

    # Create datasets for each workload
    for workload_name in workloads_list:
        datasets = []

        for data_idx, data in enumerate(all_data):
            if workload_name not in data.get("workloads", {}):
                continue

            results = data["workloads"][workload_name]["results"]
            concurrency_levels = [r["concurrency"] for r in results]
            queries_per_sec = [r["queries_per_sec"] for r in results]
            avg_latencies = [r["avg_latency_ms"] for r in results]

            label = data.get("name", f"Run {data_idx + 1}")
            if data.get("timestamp"):
                timestamp = datetime.fromisoformat(data["timestamp"])
                label += f" ({timestamp.strftime('%Y-%m-%d %H:%M')})"

            # Extract latency samples if available
            latency_samples = []
            for r in results:
                samples = r.get("latency_samples", [])
                latency_samples.append(samples)

            datasets.append(
                {
                    "label": label,
                    "concurrency_levels": concurrency_levels,
                    "throughput": queries_per_sec,
                    "avg_latencies": avg_latencies,
                    "latency_samples": latency_samples,
                }
            )

        if datasets:
            # Create comparison plot for this workload
            title = f"PostgreSQL Workload Benchmark Comparison: {workload_name}"
            if output_name:
                title += f" - {output_name}"
            title += f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"pg_workload_bench_comparison_{workload_name}_{timestamp}"
            if output_name:
                base_name += f"_{output_name}"

            plot_filename = create_comparison_violin_plots(title, datasets, base_name, plot_format)
            click.echo(f"\n✅ Comparison plot for {workload_name} saved to: {plot_filename}")


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
    multiple=True,
    help="PostgreSQL connection URL (can be specified multiple times for round-robin distribution)",
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
    "--duration",
    type=str,
    default=None,
    help="Override duration from INI file (e.g., 10s, 5m, 1h). "
    "When specified, overrides the duration value from the INI configuration.",
)
@click.option(
    "--concurrency",
    type=int,
    default=None,
    help="Override concurrency from INI file. When specified, forces single-mode "
    "with this concurrency level, ignoring any concurrency-min/max settings in the INI.",
)
@click.option(
    "--concurrency-min",
    type=int,
    default=None,
    help="Override minimum concurrency from INI file. When specified with "
    "--concurrency-max, enables range mode and overrides INI values.",
)
@click.option(
    "--concurrency-max",
    type=int,
    default=None,
    help="Override maximum concurrency from INI file. When specified with "
    "--concurrency-min, enables range mode and overrides INI values.",
)
@click.option(
    "--wait-between-runs",
    type=int,
    default=None,
    help="Override wait time in seconds between different concurrency runs. "
    "When specified, overrides the wait-between-runs value from the INI configuration "
    "(default: 20 if not in INI).",
)
@click.option(
    "--plot-format",
    type=click.Choice(["png", "svg"], case_sensitive=False),
    default="png",
    help="Format for output plots (default: png)",
)
def run(
    url: tuple[str, ...],
    workload: str,
    name: Optional[str],
    duration: Optional[str],
    concurrency: Optional[int],
    concurrency_min: Optional[int],
    concurrency_max: Optional[int],
    wait_between_runs: Optional[int],
    plot_format: str,
):
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
    # Convert tuple to list
    urls = list(url)

    try:
        asyncio.run(
            run_async(
                urls,
                workload,
                name,
                duration,
                concurrency,
                concurrency_min,
                concurrency_max,
                wait_between_runs,
                plot_format,
            )
        )
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
@click.option(
    "--plot-format",
    type=click.Choice(["png", "svg"], case_sensitive=False),
    default="png",
    help="Format for output plots (default: png)",
)
def plot(json_files: tuple[str, ...], output_name: Optional[str], plot_format: str):
    """Create comparison plots from multiple benchmark JSON files.

    Example:
        pg-workload-bench plot result1.json result2.json result3.json
    """
    plot_from_json(list(json_files), output_name, plot_format)


if __name__ == "__main__":
    try:
        cli()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # Exit cleanly without showing the traceback
        sys.exit(0)
