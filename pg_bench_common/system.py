"""System utilities for PostgreSQL benchmarking tools."""

import asyncio
import resource
import signal
from typing import Callable

import click


def adjust_file_descriptor_limits(needed_fds: int, quiet: bool = False) -> None:
    """Adjust file descriptor limits if needed.

    Args:
        needed_fds: Number of file descriptors needed
        quiet: If True, suppress output messages
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    if soft < needed_fds:
        try:
            new_limit = min(needed_fds, hard)
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


def setup_signal_handlers(shutdown_callback: Callable[[int], None]) -> None:
    """Setup signal handlers for graceful shutdown.

    Args:
        shutdown_callback: Function to call when signal is received.
                          Should accept signal number as parameter.
    """
    loop = asyncio.get_event_loop()

    def signal_handler(sig):
        shutdown_callback(sig)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))


def get_current_fd_limit() -> int:
    """Get current file descriptor limit."""
    return resource.getrlimit(resource.RLIMIT_NOFILE)[0]
