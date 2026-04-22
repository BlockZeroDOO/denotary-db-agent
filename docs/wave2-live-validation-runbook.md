# Wave 2 Live Validation Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook explains how to prepare real-environment credentials for the current `Wave 2` live harnesses and execute them through one unified entry point.

Current scope:

- `IBM Db2`
- `Apache Cassandra`
- `Elasticsearch`

## 1. Prepare Environment Variables

Start from:

- [examples/wave2-live.env.example](../examples/wave2-live.env.example)
- [examples/wave2-db2.env.example](../examples/wave2-db2.env.example)
- [examples/wave2-cassandra.env.example](../examples/wave2-cassandra.env.example)
- [examples/wave2-elasticsearch.env.example](../examples/wave2-elasticsearch.env.example)

Create a local secret env file outside version control, or export variables directly in your shell.

Only fill the adapter block you actually plan to validate.

The unified launcher can read that file directly through `--env-file`.

If you want a smaller adapter-specific template first, the launcher can generate one for the selected adapters through `--write-env-template`.

If you prefer a file-first flow, you can also bootstrap a local env file directly from one committed adapter example.

PowerShell:

```powershell
scripts/bootstrap-wave2-live-env.ps1 db2 .\wave2-db2.env
```

Shell:

```bash
scripts/bootstrap-wave2-live-env.sh db2 ./wave2-db2.env
```

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
scripts/run-wave2-live-validation.ps1 db2
scripts/run-wave2-live-validation.ps1 cassandra
scripts/run-wave2-live-validation.ps1 elasticsearch
scripts/run-wave2-live-validation.ps1 all -ListRequiredEnv
scripts/run-wave2-live-validation.ps1 db2 -WriteEnvTemplate .\wave2-db2.env
scripts/run-wave2-live-validation.ps1 all -EnvFile .\wave2-live.env -CheckEnvOnly
scripts/run-wave2-live-validation.ps1 all -StrictEnv
scripts/run-wave2-live-validation.ps1 all -OutputRoot data/wave2-live-validation-latest
```

Shell:

```bash
scripts/run-wave2-live-validation.sh all
scripts/run-wave2-live-validation.sh db2
scripts/run-wave2-live-validation.sh cassandra
scripts/run-wave2-live-validation.sh elasticsearch
scripts/run-wave2-live-validation.sh all "" --list-required-env "" text
scripts/run-wave2-live-validation.sh db2 "" --write-env-template "" json ./wave2-db2.env
scripts/run-wave2-live-validation.sh all ./wave2-live.env --check-env-only
scripts/run-wave2-live-validation.sh all ./wave2-live.env --strict-env
scripts/run-wave2-live-validation.sh all ./wave2-live.env "" data/wave2-live-validation-latest
```

Python:

```bash
python scripts/run-wave2-live-validation.py --adapter all
python scripts/run-wave2-live-validation.py --adapter db2
python scripts/run-wave2-live-validation.py --adapter all --list-required-env
python scripts/run-wave2-live-validation.py --adapter db2 --write-env-template .\wave2-db2.env
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

`--list-required-env`

- prints the exact required env names for the selected adapters
- useful before preparing credentials or CI secrets

`--write-env-template`

- creates a minimal dotenv file for the selected adapters
- useful when you do not want to start from the full shared example

`bootstrap-wave2-live-env.*`

- copies one of the committed adapter-specific examples into a local working env file
- useful when you want a file-first setup before filling credentials

`failed`

- the suite started and found a real runtime problem
- or `--strict-env` was used and required variables were missing

Every run also stores:

- `summary.json`
- `status_counts`

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
