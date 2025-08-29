#!/usr/bin/env python3
"""PostgreSQL connection load testing tool."""

import asyncio
import sys
import time
from typing import Optional

import asyncpg
import click

from pg_bench_common.database import format_target_list, setup_database_connection
from pg_bench_common.system import (
    adjust_file_descriptor_limits,
    get_current_fd_limit,
    setup_signal_handlers,
)


class ConnectionPool:
    """Manages a pool of idle PostgreSQL connections."""

    def __init__(
        self,
        postgres_url: str,
        num_connections: int,
        batch_size: int = 20,
        batch_pause: float = 0.0,
        keep_alive_interval: int = 60,
    ):
        self.postgres_url = postgres_url
        self.num_connections = num_connections
        self.batch_size = batch_size
        self.batch_pause = batch_pause
        self.keep_alive_interval = keep_alive_interval
        self.connections: list[Optional[asyncpg.Connection]] = []
        self.keep_alive_tasks: list[Optional[asyncio.Task]] = []
        self.shutdown = False
        self.keep_alive_count = 0
        self.keep_alive_errors = 0

    async def open_connections(self) -> None:
        """Open the specified number of connections to PostgreSQL in batches."""
        batch_info = f" (batch size: {self.batch_size}" + (
            f", {self.batch_pause}s pause between batches)"
            if self.batch_pause > 0
            else ")"
        )
        click.echo(
            f"Opening {self.num_connections} connections to PostgreSQL{batch_info}..."
        )

        # Calculate batches
        num_batches = (self.num_connections + self.batch_size - 1) // self.batch_size

        for batch_num in range(num_batches):
            if self.shutdown:
                break

            # Calculate batch range
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, self.num_connections)
            batch_size = end_idx - start_idx

            batch_msg = f"batch {batch_num + 1}/{num_batches}"
            click.echo(f"  Opening {batch_msg} ({batch_size} connections)...")

            # Open connections in parallel within this batch
            tasks = []
            for i in range(start_idx, end_idx):
                tasks.append(self._open_connection(i))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results for this batch
            for i, result in enumerate(results):
                connection_num = start_idx + i + 1
                if isinstance(result, Exception):
                    click.echo(
                        f"    ✗ Failed to establish connection "
                        f"{connection_num}: {result}",
                        err=True,
                    )
                    self.connections.append(None)
                else:
                    self.connections.append(result)
                    click.echo(
                        f"    ✓ Connection {connection_num}/"
                        f"{self.num_connections} established"
                    )

            # Pause between batches (except for the last batch)
            if (
                batch_num < num_batches - 1
                and self.batch_pause > 0
                and not self.shutdown
            ):
                click.echo(f"    Pausing {self.batch_pause}s before next batch...")
                await asyncio.sleep(self.batch_pause)

        successful = sum(1 for c in self.connections if c is not None)
        click.echo(
            f"\nSuccessfully opened {successful}/{self.num_connections} connections"
        )

    async def _open_connection(self, index: int) -> asyncpg.Connection:
        """Open a single connection."""
        try:
            conn = await setup_database_connection(self.postgres_url)
            return conn
        except OSError as e:
            if e.errno == 24:  # EMFILE - Too many open files
                limit = get_current_fd_limit()
                raise Exception(
                    f"Connection {index + 1}: Too many open files (EMFILE). "
                    f"Current limit: {limit}. Try running with fewer connections "
                    f"or increase ulimit."
                ) from e
            else:
                raise Exception(f"Connection {index + 1}: OS Error: {str(e)}") from e
        except asyncpg.exceptions.TooManyConnectionsError as e:
            raise Exception(
                f"Connection {index + 1}: PostgreSQL server connection "
                f"limit reached: {str(e)}"
            ) from e
        except Exception as e:
            raise Exception(f"Connection {index + 1}: {e}") from e

    async def close_connections(self) -> None:
        """Close all open connections."""
        click.echo("\nClosing connections...")
        self.shutdown = True

        tasks = []
        for i, conn in enumerate(self.connections):
            if conn is not None and not conn.is_closed():
                tasks.append(self._close_connection(conn, i))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        click.echo("All connections closed")

    async def _close_connection(self, conn: asyncpg.Connection, index: int) -> None:
        """Close a single connection."""
        try:
            await conn.close()
            click.echo(f"  ✓ Connection {index + 1} closed")
        except Exception as e:
            click.echo(f"  ✗ Error closing connection {index + 1}: {e}", err=True)

    async def _keep_alive_task(
        self, conn: asyncpg.Connection, index: int, initial_delay: float
    ) -> None:
        """Run periodic keep-alive queries on a connection."""
        # Wait for the initial staggered delay
        await asyncio.sleep(initial_delay)

        while not self.shutdown:
            try:
                # Execute keep-alive query
                start_time = time.time()
                await conn.fetchval("SELECT 1")
                latency = (time.time() - start_time) * 1000  # Convert to ms

                self.keep_alive_count += 1

                # Log every 10th keep-alive or if latency is high
                if self.keep_alive_count % 10 == 0 or latency > 100:
                    click.echo(
                        f"  Keep-alive #{self.keep_alive_count} on "
                        f"connection {index + 1}: {latency:.1f}ms"
                    )

                # Wait for the next interval
                await asyncio.sleep(self.keep_alive_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.keep_alive_errors += 1
                click.echo(
                    f"  ✗ Keep-alive error on connection {index + 1}: {e}", err=True
                )
                # Don't exit the loop, just continue with next keep-alive
                await asyncio.sleep(self.keep_alive_interval)

    async def start_keep_alive_tasks(self) -> None:
        """Start staggered keep-alive tasks for all active connections."""
        if self.keep_alive_interval <= 0:
            return

        active_connections = [
            (i, conn) for i, conn in enumerate(self.connections) if conn is not None
        ]
        if not active_connections:
            return

        click.echo(
            f"\nStarting keep-alive queries (interval: {self.keep_alive_interval}s)"
        )
        click.echo(
            f"Staggering {len(active_connections)} connections across "
            f"{self.keep_alive_interval}s interval..."
        )

        # Calculate stagger delay for each connection
        stagger_interval = self.keep_alive_interval / len(active_connections)

        for conn_index, (i, conn) in enumerate(active_connections):
            initial_delay = conn_index * stagger_interval
            task = asyncio.create_task(self._keep_alive_task(conn, i, initial_delay))
            self.keep_alive_tasks.append(task)

            if (
                conn_index < 5 or conn_index == len(active_connections) - 1
            ):  # Show first few and last
                click.echo(
                    f"  Connection {i + 1} will start keep-alive at "
                    f"+{initial_delay:.1f}s"
                )

        if len(active_connections) > 6:
            click.echo(f"  ... ({len(active_connections) - 6} more connections)")

        click.echo(
            f"Keep-alive tasks started with {stagger_interval:.2f}s "
            f"spacing between connections"
        )

    async def stop_keep_alive_tasks(self) -> None:
        """Stop all keep-alive tasks."""
        if not self.keep_alive_tasks:
            return

        for task in self.keep_alive_tasks:
            if task and not task.done():
                task.cancel()

        # Wait for all tasks to complete
        if self.keep_alive_tasks:
            await asyncio.gather(
                *[t for t in self.keep_alive_tasks if t], return_exceptions=True
            )


async def run_async(
    url: str,
    connections: int,
    duration: int,
    batch_size: int,
    batch_pause: float,
    keep_alive_interval: int,
) -> None:
    """Async implementation of the connection load tester."""
    # Adjust file descriptor limits if needed
    needed_fds = connections * 2 + 1000  # Extra buffer for other file descriptors
    adjust_file_descriptor_limits(needed_fds, quiet=False)

    click.echo("PostgreSQL Connection Load Tester")
    click.echo("================================")
    targets = format_target_list([url])
    click.echo(f"Target: {targets[0]}")
    click.echo(f"Connections: {connections}")
    click.echo(f"Duration: {'infinite' if duration == 0 else f'{duration} seconds'}")
    keep_alive_msg = (
        "disabled" if keep_alive_interval == 0 else f"{keep_alive_interval} seconds"
    )
    click.echo(f"Keep-alive interval: {keep_alive_msg}")
    click.echo(f"File descriptor limit: {get_current_fd_limit()}")
    click.echo()

    pool = ConnectionPool(
        url, connections, batch_size, batch_pause, keep_alive_interval
    )

    def shutdown_handler(sig):
        click.echo("\n\nReceived interrupt signal")
        pool.shutdown = True
        # Cancel all running tasks
        loop = asyncio.get_event_loop()
        for task in asyncio.all_tasks(loop):
            task.cancel()

    setup_signal_handlers(shutdown_handler)

    try:
        await pool.open_connections()

        if sum(1 for c in pool.connections if c is not None) == 0:
            click.echo("Failed to establish any connections. Exiting.", err=True)
            sys.exit(1)

        # Start keep-alive tasks
        await pool.start_keep_alive_tasks()

        if duration > 0:
            click.echo(f"\nKeeping connections open for {duration} seconds...")
            click.echo("Press Ctrl+C to stop early")
            await asyncio.sleep(duration)
        else:
            click.echo("\nKeeping connections open indefinitely...")
            click.echo("Press Ctrl+C to stop")
            await asyncio.Event().wait()  # Wait indefinitely

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        pool.shutdown = True

        # Stop keep-alive tasks before closing connections
        await pool.stop_keep_alive_tasks()

        # Show keep-alive statistics if applicable
        if pool.keep_alive_interval > 0 and pool.keep_alive_count > 0:
            click.echo("\nKeep-alive statistics:")
            click.echo(f"  Total queries: {pool.keep_alive_count}")
            click.echo(f"  Errors: {pool.keep_alive_errors}")
            if pool.keep_alive_errors > 0:
                error_rate = (pool.keep_alive_errors / pool.keep_alive_count) * 100
                click.echo(f"  Error rate: {error_rate:.2f}%")

        await pool.close_connections()


@click.command()
@click.option(
    "--url",
    "-u",
    required=True,
    help="PostgreSQL connection URL (e.g., postgresql://user:pass@localhost/dbname)",
    envvar="DATABASE_URL",
)
@click.option(
    "--connections",
    "-n",
    type=click.IntRange(min=1),
    default=10,
    help="Number of connections to open (default: 10)",
)
@click.option(
    "--duration",
    "-d",
    type=click.IntRange(min=0),
    default=0,
    help="Duration in seconds to keep connections open (0 = infinite, default: 0)",
)
@click.option(
    "--batch-size",
    "-b",
    type=click.IntRange(min=1),
    default=20,
    help="Number of connections to open in each batch (default: 20)",
)
@click.option(
    "--batch-pause",
    "-p",
    type=click.FloatRange(min=0.0),
    default=0.0,
    help="Pause in seconds between batches (default: 0.0, no pause)",
)
@click.option(
    "--keep-alive-interval",
    "-k",
    type=click.IntRange(min=0),
    default=60,
    help="Interval in seconds for keep-alive queries, staggered across "
    "connections (0 = disabled, default: 60)",
)
def main(
    url: str,
    connections: int,
    duration: int,
    batch_size: int,
    batch_pause: float,
    keep_alive_interval: int,
):
    """Open and maintain idle PostgreSQL connections for load testing."""
    try:
        asyncio.run(
            run_async(
                url, connections, duration, batch_size, batch_pause, keep_alive_interval
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
