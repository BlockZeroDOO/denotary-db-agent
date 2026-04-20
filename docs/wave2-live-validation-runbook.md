# Wave 2 Live Validation Runbook

This runbook explains how to prepare real-environment credentials for the current `Wave 2` live harnesses and execute them through one unified entry point.

Current scope:

- `Snowflake`
- `IBM Db2`
- `Apache Cassandra`
- `Elasticsearch`

## 1. Prepare Environment Variables

Start from:

- [examples/wave2-live.env.example](../examples/wave2-live.env.example)

Create a local secret env file outside version control, or export variables directly in your shell.

Only fill the adapter block you actually plan to validate.

The unified launcher can read that file directly through `--env-file`.

## 2. Choose Validation Mode

Default mode:

- missing env is reported as `skipped`
- useful when checking which adapters are ready to validate in the current shell

Strict mode:

- missing env is treated as a failure
- useful for CI or preflight gates

## 3. Run the Unified Launcher

PowerShell:

```powershell
scripts/run-wave2-live-validation.ps1 all
scripts/run-wave2-live-validation.ps1 snowflake
scripts/run-wave2-live-validation.ps1 db2
scripts/run-wave2-live-validation.ps1 cassandra
scripts/run-wave2-live-validation.ps1 elasticsearch
scripts/run-wave2-live-validation.ps1 all -EnvFile .\wave2-live.env -CheckEnvOnly
scripts/run-wave2-live-validation.ps1 all -StrictEnv
scripts/run-wave2-live-validation.ps1 all -OutputRoot data/wave2-live-validation-latest
```

Shell:

```bash
scripts/run-wave2-live-validation.sh all
scripts/run-wave2-live-validation.sh snowflake
scripts/run-wave2-live-validation.sh db2
scripts/run-wave2-live-validation.sh cassandra
scripts/run-wave2-live-validation.sh elasticsearch
scripts/run-wave2-live-validation.sh all ./wave2-live.env --check-env-only
scripts/run-wave2-live-validation.sh all ./wave2-live.env --strict-env
scripts/run-wave2-live-validation.sh all ./wave2-live.env "" data/wave2-live-validation-latest
```

Python:

```bash
python scripts/run-wave2-live-validation.py --adapter all
python scripts/run-wave2-live-validation.py --adapter snowflake
python scripts/run-wave2-live-validation.py --adapter all --env-file .\wave2-live.env --check-env-only
python scripts/run-wave2-live-validation.py --adapter all --strict-env
python scripts/run-wave2-live-validation.py --adapter all --env-file .\wave2-live.env --strict-env
python scripts/run-wave2-live-validation.py --adapter all --output-root data/wave2-live-validation-latest
```

## 4. Interpret the Result

`passed`

- the live integration suite ran successfully for that adapter

`ready`

- `--check-env-only` confirmed that the required env bundle is complete

`skipped`

- required env variables are missing
- the output includes the exact variable names that still need to be set

`failed`

- the suite started and found a real runtime problem
- or `--strict-env` was used and required variables were missing

Every run also stores:

- `summary.json`

under either:

- `data/wave2-live-validation-<timestamp>/`

or the path you pass through `--output-root`

## 5. Next Depth After Live Baseline

After a live adapter suite passes, the next intended depth is:

- restart validation where supported
- short-soak validation where supported
- then, if justified, `Jungle4` or `denotary` validation for the adapters we decide to advance further

## Notes

- Keep real secrets out of tracked files.
- Prefer a local env file or secret manager over editing scripts.
- The unified launcher does not replace adapter-specific runbooks; it gives one clean operator entry point into the live suites that already exist.
