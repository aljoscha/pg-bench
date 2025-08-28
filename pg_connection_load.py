#!/usr/bin/env python3
"""PostgreSQL connection load testing tool."""

import signal
import sys
import threading
import time
from typing import Optional

import click
import psycopg2
from psycopg2 import OperationalError


class ConnectionPool:
    """Manages a pool of idle PostgreSQL connections."""

    def __init__(self, postgres_url: str, num_connections: int):
        self.postgres_url = postgres_url
        self.num_connections = num_connections
        self.connections: list[Optional[psycopg2.extensions.connection]] = []
        self.lock = threading.Lock()
        self.shutdown = False

    def open_connections(self) -> None:
        """Open the specified number of connections to PostgreSQL."""
        click.echo(f"Opening {self.num_connections} connections to PostgreSQL...")

        for i in range(self.num_connections):
            try:
                conn = psycopg2.connect(self.postgres_url)
                conn.set_session(autocommit=True)

                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")

                self.connections.append(conn)
                click.echo(f"  ✓ Connection {i + 1}/{self.num_connections} established")

            except OperationalError as e:
                click.echo(f"  ✗ Failed to establish connection {i + 1}: {e}", err=True)
                self.connections.append(None)
            except Exception as e:
                click.echo(
                    f"  ✗ Unexpected error for connection {i + 1}: {e}", err=True
                )
                self.connections.append(None)

        successful = sum(1 for c in self.connections if c is not None)
        click.echo(
            f"\nSuccessfully opened {successful}/{self.num_connections} connections"
        )

    def keep_alive(self) -> None:
        """Periodically send keepalive queries to maintain connections."""
        while not self.shutdown:
            time.sleep(30)

            if self.shutdown:
                break

            with self.lock:
                for _i, conn in enumerate(self.connections):
                    if conn is not None and not conn.closed:
                        try:
                            with conn.cursor() as cursor:
                                cursor.execute("SELECT 1")
                        except Exception:
                            pass

    def close_connections(self) -> None:
        """Close all open connections."""
        click.echo("\nClosing connections...")
        self.shutdown = True

        with self.lock:
            for i, conn in enumerate(self.connections):
                if conn is not None and not conn.closed:
                    try:
                        conn.close()
                        click.echo(f"  ✓ Connection {i + 1} closed")
                    except Exception as e:
                        click.echo(
                            f"  ✗ Error closing connection {i + 1}: {e}", err=True
                        )

        click.echo("All connections closed")


def signal_handler(pool: ConnectionPool):
    """Create a signal handler that closes connections on interrupt."""

    def handler(sig, frame):
        click.echo("\n\nReceived interrupt signal")
        pool.close_connections()
        sys.exit(0)

    return handler


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
    type=click.IntRange(min=1, max=10000),
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

    click.echo("PostgreSQL Connection Load Tester")
    click.echo("================================")
    click.echo(f"Target: {url.split('@')[-1] if '@' in url else url}")
    click.echo(f"Connections: {connections}")
    click.echo(f"Duration: {'infinite' if duration == 0 else f'{duration} seconds'}")
    click.echo()

    pool = ConnectionPool(url, connections)

    signal.signal(signal.SIGINT, signal_handler(pool))
    signal.signal(signal.SIGTERM, signal_handler(pool))

    try:
        pool.open_connections()

        if sum(1 for c in pool.connections if c is not None) == 0:
            click.echo("Failed to establish any connections. Exiting.", err=True)
            sys.exit(1)

        keepalive_thread = threading.Thread(target=pool.keep_alive, daemon=True)
        keepalive_thread.start()

        if duration > 0:
            click.echo(f"\nKeeping connections open for {duration} seconds...")
            click.echo("Press Ctrl+C to stop early")
            time.sleep(duration)
        else:
            click.echo("\nKeeping connections open indefinitely...")
            click.echo("Press Ctrl+C to stop")
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        pool.close_connections()


if __name__ == "__main__":
    main()
