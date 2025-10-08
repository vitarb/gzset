#!/usr/bin/env python3

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Tuple

CRITERION_DIR = Path("target/criterion")
SUMMARY_PATH = Path(os.environ.get("GITHUB_STEP_SUMMARY", "bench-summary.md"))


def format_duration(seconds: float) -> str:
    if seconds >= 1.0:
        return f"{seconds:.2f} s"
    milliseconds = seconds * 1_000.0
    if milliseconds >= 1.0:
        return f"{milliseconds:.2f} ms"
    microseconds = milliseconds * 1_000.0
    return f"{microseconds:.2f} µs"


def collect_estimates() -> List[Tuple[str, float, float]]:
    if not CRITERION_DIR.exists():
        return []

    results: List[Tuple[str, float, float]] = []
    for estimate_path in CRITERION_DIR.rglob("new/estimates.json"):
        try:
            data = json.loads(estimate_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue

        mean = float(data["mean"]["point_estimate"]) / 1_000_000_000.0
        std_dev = float(data["std_dev"]["point_estimate"]) / 1_000_000_000.0
        bench_name_parts = estimate_path.relative_to(CRITERION_DIR).parts
        bench_name = "/".join(bench_name_parts[:-2])
        results.append((bench_name, mean, std_dev))

    results.sort(key=lambda item: item[0])
    return results


def main() -> None:
    estimates = collect_estimates()
    if not estimates:
        SUMMARY_PATH.write_text("No benchmark results found.\n")
        return

    lines = ["Benchmark summary", "", "| Benchmark | Mean | Std Dev |", "| --- | --- | --- |"]
    for name, mean, std_dev in estimates:
        lines.append(
            f"| `{name}` | {format_duration(mean)} | {format_duration(std_dev)} |")

    SUMMARY_PATH.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
