#!/usr/bin/env python3
"""PostgreSQL connection load testing tool."""

import asyncio
import resource
import signal
import sys
from typing import Optional

import asyncpg
import click


class ConnectionPool:
    """Manages a pool of idle PostgreSQL connections."""

    def __init__(self, postgres_url: str, num_connections: int):
        self.postgres_url = postgres_url
        self.num_connections = num_connections
        self.connections: list[Optional[asyncpg.Connection]] = []
        self.shutdown = False

    async def open_connections(self) -> None:
        """Open the specified number of connections to PostgreSQL."""
        click.echo(f"Opening {self.num_connections} connections to PostgreSQL...")

        tasks = []
        for i in range(self.num_connections):
            tasks.append(self._open_connection(i))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                click.echo(
                    f"  ✗ Failed to establish connection {i + 1}: {result}", err=True
                )
                self.connections.append(None)
            else:
                self.connections.append(result)
                click.echo(f"  ✓ Connection {i + 1}/{self.num_connections} established")

        successful = sum(1 for c in self.connections if c is not None)
        click.echo(
            f"\nSuccessfully opened {successful}/{self.num_connections} connections"
        )

    async def _open_connection(self, index: int) -> asyncpg.Connection:
        """Open a single connection."""
        try:
            conn = await asyncpg.connect(self.postgres_url)
            await conn.execute("SELECT 1")
            return conn
        except OSError as e:
            if e.errno == 24:  # EMFILE - Too many open files
                limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
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


async def run_async(url: str, connections: int, duration: int) -> None:
    """Async implementation of the connection load tester."""
    # Adjust file descriptor limits if needed
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    needed = connections * 2 + 1000  # Extra buffer for other file descriptors

    if soft < needed:
        try:
            new_limit = min(needed, hard)
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_limit, hard))
            click.echo(f"Increased file descriptor limit from {soft} to {new_limit}")
        except Exception as e:
            click.echo(
                f"Warning: Could not increase file descriptor limit: {e}", err=True
            )
            click.echo(
                f"Current limit is {soft}, you may hit 'too many open files' errors",
                err=True,
            )

    click.echo("PostgreSQL Connection Load Tester")
    click.echo("================================")
    click.echo(f"Target: {url.split('@')[-1] if '@' in url else url}")
    click.echo(f"Connections: {connections}")
    click.echo(f"Duration: {'infinite' if duration == 0 else f'{duration} seconds'}")
    click.echo(
        f"File descriptor limit: {resource.getrlimit(resource.RLIMIT_NOFILE)[0]}"
    )
    click.echo()

    pool = ConnectionPool(url, connections)

    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()

    def signal_handler(sig):
        click.echo("\n\nReceived interrupt signal")
        pool.shutdown = True
        # Cancel all running tasks
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    try:
        await pool.open_connections()

        if sum(1 for c in pool.connections if c is not None) == 0:
            click.echo("Failed to establish any connections. Exiting.", err=True)
            sys.exit(1)

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
def main(url: str, connections: int, duration: int):
    """Open and maintain idle PostgreSQL connections for load testing."""
    try:
        asyncio.run(run_async(url, connections, duration))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
