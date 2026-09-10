"""Windows-friendly local demo script for the Azure Lakehouse Data Pipeline"""

import subprocess
import sys
import time
from pathlib import Path


def run_command(cmd: list, description: str) -> bool:
    """Run a command and report status."""
    print(f"\n{'='*70}")
    print(f"▶ {description}")
    print(f"{'='*70}")
    print(f"Command: {' '.join(cmd)}\n")
    
    try:
        result = subprocess.run(cmd, check=True, cwd=str(Path.cwd()))
        print(f"\n✓ {description} - SUCCESS")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} - FAILED (exit code: {e.returncode})")
        return False
    except FileNotFoundError as e:
        print(f"\n✗ {description} - COMMAND NOT FOUND: {e}")
        return False


def main():
    """Run the complete demo."""
    print("""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   Azure Lakehouse Data Pipeline - Demo                        ║
║                          (Windows-Friendly)                                   ║
╚═══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    steps = [
        (
            [sys.executable, "scripts/generate_sample_data.py", "--output-dir", "data/raw"],
            "Step 1: Generate Synthetic Data"
        ),
        (
            [sys.executable, "scripts/run_pipeline.py", "--layer", "bronze"],
            "Step 2: Run Bronze Layer (Ingest)"
        ),
        (
            [sys.executable, "scripts/run_pipeline.py", "--layer", "silver"],
            "Step 3: Run Silver Layer (Cleanse & Validate)"
        ),
        (
            [sys.executable, "scripts/run_pipeline.py", "--layer", "gold"],
            "Step 4: Run Gold Layer (Analytics)"
        ),
        (
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
            "Step 5: Run Tests"
        ),
    ]
    
    results = []
    for cmd, description in steps:
        success = run_command(cmd, description)
        results.append((description, success))
        
        if not success:
            print(f"\n✗ Demo stopped at: {description}")
            break
        
        time.sleep(1)  # Pause between steps
    
    # Summary
    print(f"\n{'='*70}")
    print("DEMO SUMMARY")
    print(f"{'='*70}")
    
    for description, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {description}")
    
    all_passed = all(success for _, success in results)
    
    if all_passed:
        print(f"\n{'='*70}")
        print("✓ ALL STEPS COMPLETED SUCCESSFULLY!")
        print(f"{'='*70}")
        print("""
Next steps:
  1. Review generated data: ls data/warehouse/
  2. Run individual layers: python scripts/run_pipeline.py --layer silver
  3. Run linting: flake8 src/ --max-line-length=120
  4. Check coverage: pytest tests/ --cov=src --cov-report=html
        """)
        return 0
    else:
        print(f"\n{'='*70}")
        print("✗ DEMO FAILED - See errors above")
        print(f"{'='*70}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
