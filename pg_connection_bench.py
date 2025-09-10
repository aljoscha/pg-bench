#!/usr/bin/env python3
"""PostgreSQL connection throughput benchmarking tool."""

import asyncio
import time
from datetime import datetime
from typing import Optional

import asyncpg
import click

from pg_bench_common.database import format_target_list
from pg_bench_common.plotting import (
    create_throughput_latency_plots,
    create_violin_plots,
    enhance_results_with_percentiles,
    load_json_results,
    save_json_results,
)
from pg_bench_common.reporting import (
    ConnectionReporter,
    print_benchmark_summary,
    print_summary_header,
)
from pg_bench_common.statistics import BenchmarkStats, print_latency_stats
from pg_bench_common.system import (
    adjust_file_descriptor_limits,
    get_current_fd_limit,
    setup_signal_handlers,
)
from pg_bench_common.utils import generate_power_of_2_range, sample_latencies


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
                    self.stats.total_operations += 1
                    self.stats.latencies.append(latency)

            except asyncio.CancelledError:
                break
            except Exception as e:
                async with self.stats_lock:
                    self.stats.failed_operations += 1
                if not self.shutdown:
                    if isinstance(e, asyncpg.exceptions.TooManyConnectionsError):
                        await asyncio.sleep(0.1)
                    elif isinstance(e, OSError) and e.errno == 24:
                        await asyncio.sleep(0.1)
                    else:
                        await asyncio.sleep(0.01)

    def print_summary(self) -> None:
        """Print final summary statistics."""
        duration = time.time() - self.stats.start_time

        print_summary_header("Benchmark Summary")
        print_benchmark_summary(
            duration,
            self.concurrency,
            self.stats.total_operations,
            self.stats.failed_operations,
            self.stats.latencies,
            "connections",
        )

        if self.stats.latencies:
            print_latency_stats(self.stats.latencies)


async def run_single_benchmark(
    urls: list[str], concurrency: int, duration: int, quiet: bool = False
) -> tuple[float, float, list[float]]:
    """Run a single benchmark at a specific concurrency level.

    Returns:
        Tuple of (average connections per second, average latency in ms, latency samples)
    """
    needed_fds = concurrency * 4 + 1000
    adjust_file_descriptor_limits(needed_fds, quiet)

    if not quiet:
        click.echo("PostgreSQL Connection Throughput Benchmark")
        click.echo("=" * 60)
        targets = format_target_list(urls)
        if len(targets) == 1:
            click.echo(f"Target:       {targets[0]}")
        else:
            click.echo(f"Targets ({len(targets)} servers):")
            for i, target in enumerate(targets, 1):
                click.echo(f"  {i}. {target}")
        click.echo(f"Concurrency:  {concurrency} workers")
        duration_str = "indefinite" if duration == 0 else f"{duration} seconds"
        click.echo(f"Duration:     {duration_str}")
        click.echo(f"FD Limit:     {get_current_fd_limit()}")
        click.echo("=" * 60)
        click.echo()

    benchmark = ConnectionBenchmark(urls, concurrency)
    reporter = ConnectionReporter(benchmark.stats, benchmark.stats_lock)

    def shutdown_handler(sig):
        click.echo("\n\nReceived interrupt signal, shutting down...")
        benchmark.shutdown = True
        reporter.stop()
        loop = asyncio.get_event_loop()
        for task in asyncio.all_tasks(loop):
            task.cancel()

    setup_signal_handlers(shutdown_handler)

    try:
        workers = [benchmark.worker(i) for i in range(concurrency)]
        reporter_task = asyncio.create_task(reporter.run())

        if duration > 0:
            # Create tasks for workers
            worker_tasks = [asyncio.create_task(w) for w in workers]

            # Wait for the duration
            await asyncio.sleep(duration)

            # Signal shutdown
            benchmark.shutdown = True
            reporter.stop()

            # Cancel all tasks
            for task in worker_tasks + [reporter_task]:
                task.cancel()

            # Wait for tasks to finish cancellation
            await asyncio.gather(*worker_tasks, reporter_task, return_exceptions=True)
        else:
            await asyncio.gather(*workers, reporter_task, return_exceptions=True)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        benchmark.shutdown = True
        reporter.stop()
        await asyncio.sleep(0.1)
        if not quiet:
            benchmark.print_summary()

    # Calculate and return metrics after finally block
    duration = time.time() - benchmark.stats.start_time
    avg_rate = benchmark.stats.total_operations / duration if duration > 0 else 0
    avg_latency = sum(benchmark.stats.latencies) / len(benchmark.stats.latencies) if benchmark.stats.latencies else 0
    latency_samples = sample_latencies(benchmark.stats.latencies)
    return avg_rate, avg_latency, latency_samples


async def run_async(
    urls: list[str],
    concurrency: Optional[int],
    concurrency_min: Optional[int],
    concurrency_max: Optional[int],
    duration: int,
    name: Optional[str] = None,
    wait_between_runs: int = 20,
    plot_format: str = "png",
) -> None:
    """Async implementation of the connection benchmark.

    Supports both single concurrency mode and range mode.
    """
    # Determine if we're in range mode or single mode
    if concurrency_min is not None and concurrency_max is not None:
        # Range mode
        click.echo("PostgreSQL Connection Throughput Benchmark - Range Mode")
        click.echo("=" * 60)
        targets = format_target_list(urls)
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

            avg_rate, avg_latency, samples = await run_single_benchmark(urls, c, duration, quiet=False)
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
        for c, rate, latency in zip(concurrency_levels, connections_per_sec, avg_latencies):
            click.echo(f"{c:<15} {rate:<20.1f} {latency:<20.2f}")

        # Save JSON results
        click.echo("\nSaving raw results...")
        results_data = {
            "concurrency_min": concurrency_min,
            "concurrency_max": concurrency_max,
            "duration_per_run": duration,
            "results": enhance_results_with_percentiles(
                [
                    {
                        "concurrency": c,
                        "connections_per_sec": rate,
                        "avg_latency_ms": latency,
                        "latency_samples": samples,
                    }
                    for c, rate, latency, samples in zip(
                        concurrency_levels,
                        connections_per_sec,
                        avg_latencies,
                        latency_samples,
                    )
                ]
            ),
        }
        json_filename = save_json_results(
            results_data,
            "pg_bench",
            name,
            ",".join(urls),
            concurrency_min,
            concurrency_max,
        )
        click.echo(f"✅ Raw results saved to: {json_filename}")

        # Generate and save plots
        click.echo("\nGenerating performance plots...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"pg_bench_{timestamp}"
        if name:
            base_name += f"_{name}"
        base_name += f"_c{concurrency_min}-{concurrency_max}"

        dataset = {
            "label": name or "PostgreSQL Connection Benchmark",
            "concurrency_levels": concurrency_levels,
            "throughput": connections_per_sec,
            "avg_latencies": avg_latencies,
            "latency_samples": latency_samples,
        }

        title = "PostgreSQL Connection Benchmark Results"
        if name:
            title += f": {name}"
        title += f"\n{timestamp}"

        plot_filename = create_violin_plots(title, [dataset], base_name, plot_format)
        click.echo(f"✅ Performance plots saved to: {plot_filename}")

    else:
        # Single concurrency mode
        if concurrency is None:
            concurrency = 10  # Default value
        await run_single_benchmark(urls, concurrency, duration, quiet=False)


def plot_from_json(
    json_files: list[str],
    output_name: Optional[str] = None,
    plot_format: str = "png",
) -> None:
    """Load multiple JSON result files and create combined plots."""
    try:
        all_data = load_json_results(json_files)
    except ValueError as e:
        click.echo(str(e), err=True)
        return

    # Prepare datasets for plotting
    datasets = []
    for idx, data in enumerate(all_data):
        results = data["results"]
        concurrency_levels = [r["concurrency"] for r in results]
        connections_per_sec = [r["connections_per_sec"] for r in results]
        avg_latencies = [r["avg_latency_ms"] for r in results]

        label = data.get("name", f"Run {idx + 1}")
        if data.get("timestamp"):
            timestamp = datetime.fromisoformat(data["timestamp"])
            label += f" ({timestamp.strftime('%Y-%m-%d %H:%M')})"

        datasets.append(
            {
                "label": label,
                "concurrency_levels": concurrency_levels,
                "throughput": connections_per_sec,
                "avg_latencies": avg_latencies,
            }
        )

    # Create comparison plot
    title = "PostgreSQL Connection Benchmark Comparison"
    if output_name:
        title += f": {output_name}"
    title += f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"pg_bench_comparison_{timestamp}"
    if output_name:
        base_name += f"_{output_name}"

    plot_filename = create_throughput_latency_plots(
        title, datasets, base_name, plot_format, "Connections per Second", "Average Latency (ms)"
    )
    click.echo(f"\n✅ Comparison plot saved to: {plot_filename}")


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
@click.option(
    "--plot-format",
    type=click.Choice(["png", "svg"], case_sensitive=False),
    default="png",
    help="Format for output plots (default: png)",
)
def run(
    url: tuple[str, ...],
    concurrency: Optional[int],
    concurrency_min: Optional[int],
    concurrency_max: Optional[int],
    duration: int,
    name: Optional[str],
    wait_between_runs: int,
    plot_format: str,
):
    """Run benchmarks on PostgreSQL database(s).

    Supports two modes:
    1. Single concurrency: Use --concurrency/-c
    2. Range mode: Use --concurrency-min and --concurrency-max to test multiple levels

    In range mode, tests powers of 2 between min and max values.

    When multiple URLs are provided, connections are distributed round-robin across servers.
    """
    # Validate arguments
    if concurrency is not None and (concurrency_min is not None or concurrency_max is not None):
        raise click.UsageError(
            "Cannot use --concurrency with --concurrency-min/--concurrency-max. "
            "Choose either single mode or range mode."
        )

    if (concurrency_min is not None) != (concurrency_max is not None):
        raise click.UsageError("Both --concurrency-min and --concurrency-max must be provided for range mode.")

    if concurrency_min is not None and concurrency_max is not None:
        if concurrency_min > concurrency_max:
            raise click.UsageError("--concurrency-min must be less than or equal to --concurrency-max.")

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
                plot_format,
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
@click.option(
    "--plot-format",
    type=click.Choice(["png", "svg"], case_sensitive=False),
    default="png",
    help="Format for output plots (default: png)",
)
def plot(json_files: tuple[str, ...], output_name: Optional[str], plot_format: str):
    """Create comparison plots from multiple benchmark JSON files.

    Example:
        pg-connection-bench plot result1.json result2.json result3.json
    """
    plot_from_json(list(json_files), output_name, plot_format)


if __name__ == "__main__":
    cli()
