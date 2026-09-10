"""Run a reproducible end-to-end portfolio demo on Windows, macOS, or Linux."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str], label: str) -> None:
    print(f"\n{'=' * 72}\n{label}\n{'=' * 72}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def main() -> int:
    python = sys.executable
    try:
        run(
            [
                python,
                "scripts/generate_sample_data.py",
                "--customers",
                "100",
                "--products",
                "50",
                "--orders",
                "500",
                "--cdc-events",
                "12",
                "--seed",
                "42",
            ],
            "1/3 Generate deterministic source data",
        )
        run(
            [
                python,
                "scripts/run_pipeline.py",
                "--layer",
                "full",
                "--cdc-path",
                "data/cdc/customer_cdc_events.json",
            ],
            "2/3 Run Bronze -> Silver + CDC -> Gold",
        )
        run(
            [python, "-m", "pytest", "-m", "not integration", "-q"],
            "3/3 Run deterministic transformation tests",
        )
    except subprocess.CalledProcessError as error:
        print(f"\nDEMO FAILED (exit code {error.returncode})")
        return error.returncode

    print("\nDEMO PASSED: pipeline completed, Gold reconciled, and tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
