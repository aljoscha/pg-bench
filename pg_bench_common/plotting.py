"""Plotting and JSON serialization utilities for PostgreSQL benchmarking tools."""

import json
from datetime import datetime
from typing import Any, Optional

import click
import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend
import matplotlib.pyplot as plt

from .database import sanitize_url_for_display

# Common color scheme and markers
PLOT_COLORS = [
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
PLOT_MARKERS = ["o", "s", "^", "v", "D", "p", "*", "h", "X", "+"]


def save_json_results(
    results_data: dict[str, Any],
    base_name: str,
    name: Optional[str] = None,
    url: Optional[str] = None,
    concurrency_min: Optional[int] = None,
    concurrency_max: Optional[int] = None,
) -> str:
    """Save benchmark results as JSON with standardized format.

    Args:
        results_data: Dictionary containing the specific benchmark results
        base_name: Base name for the output file
        name: Optional name for the benchmark run
        url: Database URL (will be sanitized)
        concurrency_min: Minimum concurrency tested
        concurrency_max: Maximum concurrency tested

    Returns:
        JSON filename
    """
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    filename = f"{base_name}_{timestamp_str}"

    if name:
        filename += f"_{name}"
    if concurrency_min and concurrency_max:
        filename += f"_c{concurrency_min}-{concurrency_max}"

    json_filename = f"{filename}.json"

    # Standard metadata
    output = {
        "timestamp": timestamp.isoformat(),
        "name": name,
        "url": sanitize_url_for_display(url) if url else None,
        **results_data,  # Merge in the specific results
    }

    with open(json_filename, "w") as f:
        json.dump(output, f, indent=2)

    return json_filename


def enhance_results_with_percentiles(results: list[dict]) -> list[dict]:
    """Add percentile calculations to results that have latency_samples.

    Args:
        results: List of result dictionaries

    Returns:
        Enhanced results with percentiles added
    """
    enhanced = []
    for result in results:
        enhanced_result = result.copy()
        if "latency_samples" in result and result["latency_samples"]:
            samples = sorted(result["latency_samples"])
            enhanced_result["latency_percentiles"] = {
                "p50": float(samples[len(samples) // 2]),
                "p95": float(samples[int(len(samples) * 0.95)]),
                "p99": float(samples[int(len(samples) * 0.99)]),
            }
        enhanced.append(enhanced_result)
    return enhanced


def create_throughput_latency_plots(
    title: str,
    datasets: list[dict[str, Any]],
    output_filename: str,
    throughput_ylabel: str = "Operations per Second",
    latency_ylabel: str = "Latency (ms)",
) -> str:
    """Create standardized throughput and latency plots.

    Args:
        title: Overall plot title
        datasets: List of datasets to plot, each containing:
                 - label: Display name
                 - concurrency_levels: X-axis values
                 - throughput: Y-axis values for throughput plot
                 - avg_latencies: Y-axis values for latency plot
                 - latency_samples: (optional) For violin plots
        output_filename: Base filename for output
        throughput_ylabel: Y-axis label for throughput plot
        latency_ylabel: Y-axis label for latency plot

    Returns:
        Saved filename
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Plot each dataset
    for idx, dataset in enumerate(datasets):
        color = PLOT_COLORS[idx % len(PLOT_COLORS)]
        marker = PLOT_MARKERS[idx % len(PLOT_MARKERS)]

        concurrency_levels = dataset["concurrency_levels"]
        throughput = dataset["throughput"]
        avg_latencies = dataset["avg_latencies"]
        label = dataset["label"]

        # Throughput plot
        ax1.plot(
            concurrency_levels,
            throughput,
            color=color,
            marker=marker,
            linewidth=2,
            markersize=8,
            label=label,
            alpha=0.8,
        )

        # Latency plot
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

    # Configure throughput plot
    ax1.set_xlabel("Concurrency (workers)", fontsize=12)
    ax1.set_ylabel(throughput_ylabel, fontsize=12)
    ax1.set_title("Throughput vs Concurrency", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale("log", base=2)
    ax1.set_ylim(bottom=0)
    if len(datasets) > 1:
        ax1.legend(loc="best", fontsize=9)

    # Configure latency plot
    ax2.set_xlabel("Concurrency (workers)", fontsize=12)
    ax2.set_ylabel(latency_ylabel, fontsize=12)
    ax2.set_title("Average Latency vs Concurrency", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.set_xscale("log", base=2)
    ax2.set_ylim(bottom=0)
    if len(datasets) > 1:
        ax2.legend(loc="best", fontsize=9)

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()

    return _save_plot(output_filename, fig)


def create_comparison_violin_plots(
    title: str,
    datasets: list[dict[str, Any]],
    output_filename: str,
    throughput_ylabel: str = "Queries per Second",
    latency_ylabel: str = "Latency (ms)",
) -> str:
    """Create comparison plots with throughput lines and violin plots for latency.

    Args:
        title: Overall plot title
        datasets: List of datasets to plot, each containing:
                 - label: Display name
                 - concurrency_levels: X-axis values
                 - throughput: Y-axis values for throughput plot
                 - avg_latencies: Y-axis values for latency plot
                 - latency_samples: For violin plots
        output_filename: Base filename for output
        throughput_ylabel: Y-axis label for throughput plot
        latency_ylabel: Y-axis label for latency plot

    Returns:
        Saved filename
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # Get all unique concurrency levels across datasets
    all_concurrency = set()
    for dataset in datasets:
        all_concurrency.update(dataset["concurrency_levels"])
    sorted_concurrency = sorted(all_concurrency)

    # Plot throughput with lines for each dataset
    for idx, dataset in enumerate(datasets):
        color = PLOT_COLORS[idx % len(PLOT_COLORS)]
        marker = PLOT_MARKERS[idx % len(PLOT_MARKERS)]

        concurrency_levels = dataset["concurrency_levels"]
        throughput = dataset["throughput"]
        label = dataset["label"]

        # Throughput plot
        ax1.plot(
            concurrency_levels,
            throughput,
            color=color,
            marker=marker,
            linewidth=2,
            markersize=8,
            label=label,
            alpha=0.8,
        )

        # Add value labels for throughput
        for x, y in zip(concurrency_levels, throughput):
            ax1.annotate(
                f"{y:.0f}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 8),
                ha="center",
                fontsize=7,
                color=color,
                alpha=0.9,
            )

    # Configure throughput plot
    ax1.set_xlabel("Concurrency (workers)", fontsize=12)
    ax1.set_ylabel(throughput_ylabel, fontsize=12)
    ax1.set_title("Throughput Comparison", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale("log", base=2)
    ax1.set_xticks(sorted_concurrency)
    ax1.set_xticklabels([str(c) for c in sorted_concurrency], rotation=45)
    ax1.set_ylim(bottom=0)
    if len(datasets) > 1:
        ax1.legend(loc="best", fontsize=9)

    # Prepare data for violin plots
    violin_data_by_concurrency = {}
    dataset_labels = []

    for idx, dataset in enumerate(datasets):
        dataset_labels.append(dataset["label"])
        concurrency_levels = dataset["concurrency_levels"]
        latency_samples = dataset.get("latency_samples", [])

        for c_level, samples in zip(concurrency_levels, latency_samples):
            if c_level not in violin_data_by_concurrency:
                violin_data_by_concurrency[c_level] = []
            # Pad with None for missing datasets at this concurrency
            while len(violin_data_by_concurrency[c_level]) < idx:
                violin_data_by_concurrency[c_level].append(None)
            violin_data_by_concurrency[c_level].append(samples if samples else None)

    # Ensure all concurrency levels have data for all datasets (pad with None)
    for c_level in violin_data_by_concurrency:
        while len(violin_data_by_concurrency[c_level]) < len(datasets):
            violin_data_by_concurrency[c_level].append(None)

    # Create violin plots grouped by concurrency level
    positions = []
    labels_for_xticks = []
    violin_position = 0
    group_width = 0.8

    for _c_idx, c_level in enumerate(sorted_concurrency):
        if c_level in violin_data_by_concurrency:
            samples_list = violin_data_by_concurrency[c_level]
            num_datasets_at_level = sum(1 for s in samples_list if s is not None)

            if num_datasets_at_level > 0:
                # Calculate positions for this concurrency level
                if num_datasets_at_level == 1:
                    dataset_positions = [violin_position]
                else:
                    step = group_width / (num_datasets_at_level - 1)
                    dataset_positions = [
                        violin_position - group_width/2 + i * step
                        for i in range(num_datasets_at_level)
                    ]

                # Draw violin for each dataset at this concurrency
                pos_idx = 0
                for dataset_idx, samples in enumerate(samples_list):
                    if samples and len(samples) > 0:
                        color = PLOT_COLORS[dataset_idx % len(PLOT_COLORS)]
                        pos = dataset_positions[pos_idx]
                        positions.append(pos)

                        parts = ax2.violinplot(
                            [samples],
                            positions=[pos],
                            widths=group_width / (num_datasets_at_level + 1),
                            showmeans=True,
                            showextrema=True,
                            showmedians=True,
                        )

                        for pc in parts["bodies"]:
                            pc.set_facecolor(color)
                            pc.set_alpha(0.6)

                        # Color the other parts
                        for partname in ["cmeans", "cmaxes", "cmins", "cbars", "cmedians"]:
                            if partname in parts:
                                parts[partname].set_color(color)
                                parts[partname].set_alpha(0.8)

                        # Add median annotation
                        median = sorted(samples)[len(samples) // 2]
                        ax2.annotate(
                            f"{median:.1f}",
                            (pos, median),
                            textcoords="offset points",
                            xytext=(0, -12),
                            ha="center",
                            fontsize=7,
                            color=color,
                            fontweight="bold",
                        )

                        pos_idx += 1

                labels_for_xticks.append((violin_position, str(c_level)))
                violin_position += 1.5

    # Configure latency plot
    ax2.set_xlabel("Concurrency (workers)", fontsize=12)
    ax2.set_ylabel(latency_ylabel, fontsize=12)
    ax2.set_title("Latency Distribution Comparison", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3, axis="y")

    # Set x-ticks at the center of each concurrency group
    if labels_for_xticks:
        tick_positions, tick_labels = zip(*labels_for_xticks)
        ax2.set_xticks(tick_positions)
        ax2.set_xticklabels(tick_labels, rotation=45)

    ax2.set_ylim(bottom=0)

    # Add legend for violin plot
    if len(datasets) > 1:
        # Create dummy lines for legend
        legend_elements = []
        for idx, label in enumerate(dataset_labels):
            color = PLOT_COLORS[idx % len(PLOT_COLORS)]
            from matplotlib.patches import Patch
            legend_elements.append(Patch(facecolor=color, alpha=0.6, label=label))
        ax2.legend(handles=legend_elements, loc="best", fontsize=9)

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()

    return _save_plot(output_filename, fig)


def create_violin_plots(
    title: str,
    datasets: list[dict[str, Any]],
    output_filename: str,
) -> str:
    """Create violin plots for latency distributions.

    Args:
        title: Overall plot title
        datasets: List of datasets with latency_samples
        output_filename: Base filename for output

    Returns:
        Saved filename
    """
    num_datasets = len(datasets)
    fig, axes = plt.subplots(num_datasets, 2, figsize=(14, 6 * num_datasets))

    if num_datasets == 1:
        axes = axes.reshape(1, 2)

    for idx, dataset in enumerate(datasets):
        color = PLOT_COLORS[idx % len(PLOT_COLORS)]

        concurrency_levels = dataset["concurrency_levels"]
        throughput = dataset["throughput"]
        latency_samples = dataset.get("latency_samples", [])
        label = dataset["label"]

        ax1 = axes[idx, 0]
        ax2 = axes[idx, 1]

        # Throughput plot
        ax1.plot(
            concurrency_levels,
            throughput,
            color=color,
            marker="o",
            linewidth=2,
            markersize=8,
        )
        ax1.set_xlabel("Concurrency (workers)", fontsize=11)
        ax1.set_ylabel("Operations per Second", fontsize=11)
        ax1.set_title(f"Throughput: {label}", fontsize=12, fontweight="bold")
        ax1.grid(True, alpha=0.3)
        ax1.set_xscale("log", base=2)
        ax1.set_xticks(concurrency_levels)
        ax1.set_xticklabels([str(c) for c in concurrency_levels], rotation=45)
        ax1.set_ylim(bottom=0)

        # Add value labels
        for x, y in zip(concurrency_levels, throughput):
            ax1.annotate(
                f"{y:.0f}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=8,
            )

        # Violin plot for latency distribution
        if latency_samples and any(len(s) > 0 for s in latency_samples):
            positions = list(range(len(concurrency_levels)))
            valid_samples = [s if s else [0] for s in latency_samples]

            parts = ax2.violinplot(
                valid_samples,
                positions=positions,
                widths=0.7,
                showmeans=True,
                showextrema=True,
                showmedians=True,
            )

            for pc in parts["bodies"]:
                pc.set_facecolor(color)
                pc.set_alpha(0.7)

            # Add median annotations
            for _i, (pos, samples) in enumerate(zip(positions, valid_samples)):
                if samples and len(samples) > 0:
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

        ax2.set_xlabel("Concurrency (workers)", fontsize=11)
        ax2.set_ylabel("Latency (ms)", fontsize=11)
        ax2.set_title(f"Latency Distribution: {label}", fontsize=12, fontweight="bold")
        ax2.grid(True, alpha=0.3, axis="y")
        ax2.set_xticks(range(len(concurrency_levels)))
        ax2.set_xticklabels([str(c) for c in concurrency_levels], rotation=45)
        ax2.set_ylim(bottom=0)

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()

    return _save_plot(output_filename, fig)


def load_json_results(json_files: list[str]) -> list[dict]:
    """Load and validate multiple JSON result files.

    Args:
        json_files: List of JSON file paths

    Returns:
        List of loaded data dictionaries
    """
    all_data = []

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
        raise ValueError("No valid JSON files loaded.")

    return all_data


def _save_plot(filename: str, fig) -> str:
    """Save plot with fallback from SVG to PNG.

    Args:
        filename: Base filename (without extension)
        fig: Matplotlib figure object

    Returns:
        Actual saved filename
    """
    try:
        svg_filename = f"{filename}.svg"
        fig.savefig(svg_filename, format="svg", dpi=150, bbox_inches="tight")
        plt.close(fig)
        return svg_filename
    except Exception as e:
        click.echo(f"Warning: Could not save as SVG ({e}), saving as PNG instead")
        png_filename = f"{filename}.png"
        fig.savefig(png_filename, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        return png_filename
