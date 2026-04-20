#!/usr/bin/env bash
set -euo pipefail

python -m unittest discover -s tests -p 'test_snowflake*_integration.py' -v
