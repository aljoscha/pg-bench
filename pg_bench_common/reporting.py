"""Real-time reporting utilities for PostgreSQL benchmarking tools."""

import asyncio
import time
from typing import Protocol

import click


class ReportableStats(Protocol):
    """Protocol for stats objects that can be reported."""

    total_operations: int
    failed_operations: int
    latencies: list[float]
    start_time: float
    last_report_time: float
    last_report_operations: int


class AsyncReporter:
    """Base class for async reporters that display real-time statistics."""

    def __init__(self, stats: ReportableStats, stats_lock: asyncio.Lock):
        self.stats = stats
        self.stats_lock = stats_lock
        self.shutdown = False

    async def run(self) -> None:
        """Run the reporter loop."""
        await asyncio.sleep(1)  # Initial delay

        while not self.shutdown:
            await self._report_stats()
            await asyncio.sleep(1)

    async def _report_stats(self) -> None:
        """Report current statistics. Override in subclasses."""
        async with self.stats_lock:
            current_time = time.time()
            time_delta = current_time - self.stats.last_report_time
            ops_delta = self.stats.total_operations - self.stats.last_report_operations

            if time_delta > 0:
                rate = ops_delta / time_delta
                elapsed = int(current_time - self.stats.start_time)

                if self.stats.latencies:
                    recent_latencies = self.stats.latencies[
                        -min(1000, len(self.stats.latencies)) :
                    ]
                    avg_latency = sum(recent_latencies) / len(recent_latencies)
                    min_latency = min(recent_latencies)
                    max_latency = max(recent_latencies)

                    click.echo(
                        self._format_report_with_latency(
                            elapsed, rate, avg_latency, min_latency, max_latency
                        )
                    )
                else:
                    click.echo(self._format_report_without_latency(elapsed, rate))

                self.stats.last_report_time = current_time
                self.stats.last_report_operations = self.stats.total_operations

    def _format_report_with_latency(
        self,
        elapsed: int,
        rate: float,
        avg_latency: float,
        min_latency: float,
        max_latency: float,
    ) -> str:
        """Format report line with latency information. Override in subclasses."""
        return (
            f"[{elapsed:>4}s] "
            f"Rate: {rate:>7.1f} ops/s | "
            f"Total: {self.stats.total_operations:>7} | "
            f"Failed: {self.stats.failed_operations:>5} | "
            f"Latency (ms): avg={avg_latency:>6.1f} "
            f"min={min_latency:>6.1f} max={max_latency:>6.1f}"
        )

    def _format_report_without_latency(self, elapsed: int, rate: float) -> str:
        """Format report line without latency information. Override in subclasses."""
        return (
            f"[{elapsed:>4}s] "
            f"Rate: {rate:>7.1f} ops/s | "
            f"Total: {self.stats.total_operations:>7} | "
            f"Failed: {self.stats.failed_operations:>5}"
        )

    def stop(self) -> None:
        """Signal the reporter to stop."""
        self.shutdown = True


class ConnectionReporter(AsyncReporter):
    """Reporter for connection benchmarks."""

    def _format_report_with_latency(
        self,
        elapsed: int,
        rate: float,
        avg_latency: float,
        min_latency: float,
        max_latency: float,
    ) -> str:
        return (
            f"[{elapsed:>4}s] "
            f"Rate: {rate:>7.1f} conn/s | "
            f"Total: {self.stats.total_operations:>7} | "
            f"Failed: {self.stats.failed_operations:>5} | "
            f"Latency (ms): avg={avg_latency:>6.1f} "
            f"min={min_latency:>6.1f} max={max_latency:>6.1f}"
        )

    def _format_report_without_latency(self, elapsed: int, rate: float) -> str:
        return (
            f"[{elapsed:>4}s] "
            f"Rate: {rate:>7.1f} conn/s | "
            f"Total: {self.stats.total_operations:>7} | "
            f"Failed: {self.stats.failed_operations:>5}"
        )


class WorkloadReporter(AsyncReporter):
    """Reporter for workload benchmarks."""

    def __init__(
        self, stats: ReportableStats, stats_lock: asyncio.Lock, workload_name: str
    ):
        super().__init__(stats, stats_lock)
        self.workload_name = workload_name

    def _format_report_with_latency(
        self,
        elapsed: int,
        rate: float,
        avg_latency: float,
        min_latency: float,
        max_latency: float,
    ) -> str:
        return (
            f"[{elapsed:>4}s] "
            f"Workload: {self.workload_name:<20} | "
            f"Rate: {rate:>7.1f} q/s | "
            f"Total: {self.stats.total_operations:>7} | "
            f"Failed: {self.stats.failed_operations:>5} | "
            f"Latency (ms): avg={avg_latency:>6.1f} "
            f"min={min_latency:>6.1f} max={max_latency:>6.1f}"
        )

    def _format_report_without_latency(self, elapsed: int, rate: float) -> str:
        return (
            f"[{elapsed:>4}s] "
            f"Workload: {self.workload_name:<20} | "
            f"Rate: {rate:>7.1f} q/s | "
            f"Total: {self.stats.total_operations:>7} | "
            f"Failed: {self.stats.failed_operations:>5}"
        )


def print_summary_header(title: str) -> None:
    """Print a standardized summary header."""
    click.echo("\n" + "=" * 60)
    click.echo(title)
    click.echo("=" * 60)


def print_benchmark_summary(
    duration: float,
    concurrency: int,
    total_operations: int,
    failed_operations: int,
    latencies: list[float],
    operation_name: str = "operations",
) -> None:
    """Print standardized benchmark summary statistics.

    Args:
        duration: Benchmark duration in seconds
        concurrency: Number of concurrent workers
        total_operations: Total operations completed
        failed_operations: Number of failed operations
        latencies: List of operation latencies
        operation_name: Name for operations (e.g., "connections", "queries")
    """
    click.echo(f"Duration:             {duration:.1f} seconds")
    click.echo(f"Concurrency:          {concurrency} workers")
    click.echo(f"Total {operation_name.title()}:   {total_operations}")
    click.echo(f"Failed {operation_name.title()}:  {failed_operations}")

    if total_operations > 0:
        success_rate = (total_operations - failed_operations) / total_operations * 100
        click.echo(f"Success Rate:         {success_rate:.1f}%")

    if duration > 0:
        avg_rate = total_operations / duration
        click.echo(f"Average Rate:         {avg_rate:.1f} {operation_name}/second")
