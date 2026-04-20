#!/usr/bin/env bash
set -euo pipefail

python -m unittest discover -s tests -p 'test_db2*_integration.py' -v
