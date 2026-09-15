#!/usr/bin/env python3
"""Entry point for the lab; run `./ab --help` from the repository root."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ablab.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
