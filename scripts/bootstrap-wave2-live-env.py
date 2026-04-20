from __future__ import annotations

import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_ROOT = PROJECT_ROOT / "examples"

ADAPTER_EXAMPLES = {
    "snowflake": EXAMPLES_ROOT / "wave2-snowflake.env.example",
    "db2": EXAMPLES_ROOT / "wave2-db2.env.example",
    "cassandra": EXAMPLES_ROOT / "wave2-cassandra.env.example",
    "elasticsearch": EXAMPLES_ROOT / "wave2-elasticsearch.env.example",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap a local Wave 2 live env file from an adapter example.")
    parser.add_argument("--adapter", choices=tuple(ADAPTER_EXAMPLES), required=True)
    parser.add_argument("--output", required=True, help="Target env file path to create.")
    parser.add_argument("--force", action="store_true", help="Overwrite the target file if it already exists.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = ADAPTER_EXAMPLES[args.adapter]
    target = Path(args.output).resolve()
    if target.exists() and not args.force:
        raise SystemExit(
            f"Target already exists: {target}. Use --force if you want to overwrite it."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    print(
        json.dumps(
            {
                "adapter": args.adapter,
                "source": str(source),
                "output": str(target),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
