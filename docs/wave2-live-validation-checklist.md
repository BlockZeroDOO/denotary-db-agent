# Wave 2 Live Validation Checklist

This checklist provides one operator-facing entry point for the `Wave 2` adapters that already have env-gated live harnesses.

Current scope:

- `Snowflake`
- `IBM Db2`
- `Apache Cassandra`
- `Elasticsearch`

The goal is to make live validation repeatable without having to remember four different script names and four different env bundles.

For a copy-ready env template and step-by-step operator flow, also see:

- [wave2-live-validation-runbook.md](wave2-live-validation-runbook.md)
- [../examples/wave2-live.env.example](../examples/wave2-live.env.example)

## Unified Commands

PowerShell:

```powershell
scripts/run-wave2-live-validation.ps1 all
scripts/run-wave2-live-validation.ps1 snowflake
scripts/run-wave2-live-validation.ps1 db2
scripts/run-wave2-live-validation.ps1 cassandra
scripts/run-wave2-live-validation.ps1 elasticsearch
scripts/run-wave2-live-validation.ps1 all -EnvFile .\wave2-live.env -CheckEnvOnly
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
scripts/run-wave2-live-validation.sh all ./wave2-live.env "" data/wave2-live-validation-latest
```

Python:

```bash
python scripts/run-wave2-live-validation.py --adapter all
python scripts/run-wave2-live-validation.py --adapter snowflake
python scripts/run-wave2-live-validation.py --adapter all --env-file .\wave2-live.env --check-env-only
python scripts/run-wave2-live-validation.py --adapter all --output-root data/wave2-live-validation-latest
```

Use `--strict-env` when missing credentials should fail the run instead of being reported as `skipped`.

Use `--env-file` to load adapter credentials from a dotenv-style file instead of exporting them into the shell first.

Use `--check-env-only` to verify readiness without executing the live suites.

By default the launcher stores a persistent summary under:

- `data/wave2-live-validation-<timestamp>/summary.json`

## Required Environment

### Snowflake

- `DENOTARY_SNOWFLAKE_ACCOUNT`
- `DENOTARY_SNOWFLAKE_USERNAME`
- `DENOTARY_SNOWFLAKE_PASSWORD`
- `DENOTARY_SNOWFLAKE_DATABASE`
- `DENOTARY_SNOWFLAKE_SCHEMA`
- `DENOTARY_SNOWFLAKE_WAREHOUSE`

Optional:

- `DENOTARY_SNOWFLAKE_ROLE`

### IBM Db2

- `DENOTARY_DB2_HOST`
- `DENOTARY_DB2_PORT`
- `DENOTARY_DB2_USERNAME`
- `DENOTARY_DB2_PASSWORD`
- `DENOTARY_DB2_DATABASE`
- `DENOTARY_DB2_SCHEMA`

### Apache Cassandra

- `DENOTARY_CASSANDRA_HOST`
- `DENOTARY_CASSANDRA_PORT`
- `DENOTARY_CASSANDRA_KEYSPACE`

Optional:

- `DENOTARY_CASSANDRA_USERNAME`
- `DENOTARY_CASSANDRA_PASSWORD`

### Elasticsearch

- `DENOTARY_ELASTICSEARCH_URL`

Optional:

- `DENOTARY_ELASTICSEARCH_USERNAME`
- `DENOTARY_ELASTICSEARCH_PASSWORD`
- `DENOTARY_ELASTICSEARCH_VERIFY_CERTS`

## Expected Outcome

For each adapter:

1. the script checks whether the required env bundle is present
2. if env is missing:
   - default mode reports `skipped`
   - `--strict-env` reports `failed`
3. with `--check-env-only`:
   - complete adapter bundles report `ready`
4. if env is present and suites are enabled:
   - the corresponding live integration suite is executed
   - the result is reported as `passed` or `failed`

## Interpretation

Passing this checklist means:

- the adapter can be validated against a real target environment using the current live harness
- operators have one consistent entry point for `Wave 2` live validation

It does not yet mean:

- the adapter is already validated on `Jungle4` or `denotary`
- restart, soak, service-outage, or mainnet depth is complete for every `Wave 2` adapter
