# deNotary DB Agent Deployment Guide

This guide describes the supported production packaging patterns for `denotary-db-agent`.

For a `denotary` mainnet deployment with an env-file-backed hot key, use:

- [config-reference.md](config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [postgresql-config-reference.md](postgresql-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)
- [denotary-postgresql-rollout-checklist.md](denotary-postgresql-rollout-checklist.md)
- [denotary-postgresql-validation-report.md](denotary-postgresql-validation-report.md)
- [jungle4-postgresql-short-soak-report.md](jungle4-postgresql-short-soak-report.md)
- [jungle4-postgresql-soak-runbook.md](jungle4-postgresql-soak-runbook.md)
- [postgresql-hot-key-rotation.md](postgresql-hot-key-rotation.md)
- [postgresql-recovery-scenarios.md](postgresql-recovery-scenarios.md)

For an edge or embedded `SQLite` deployment baseline, use:

- [sqlite-config-reference.md](sqlite-config-reference.md)
- [wave2-sqlite-edge-runbook.md](wave2-sqlite-edge-runbook.md)
- [deploy/config/sqlite-edge-agent.example.json](../deploy/config/sqlite-edge-agent.example.json)

## Recommended Modes

- `systemd` on Linux servers
- Windows Service on Windows hosts
- Docker Compose for containerized deployments

In every mode, the agent should run in daemon mode:

```bash
denotary-db-agent --config /path/to/agent.json run --interval-sec 5
```

## Linux systemd

Template:

- [deploy/systemd/denotary-db-agent.service.example](../deploy/systemd/denotary-db-agent.service.example)

Recommended layout:

- code: `/opt/denotary-db-agent`
- config: `/etc/denotary-db-agent/agent.json`
- runtime state and proofs: configured inside `storage`
- starter config pack:
  - [deploy/config/linux-agent.example.json](../deploy/config/linux-agent.example.json)

Typical install flow:

```bash
python -m venv /opt/denotary-db-agent/.venv
/opt/denotary-db-agent/.venv/bin/pip install /opt/denotary-db-agent
sudo cp deploy/systemd/denotary-db-agent.service.example /etc/systemd/system/denotary-db-agent.service
sudo systemctl daemon-reload
sudo systemctl enable denotary-db-agent
sudo systemctl start denotary-db-agent
```

The example unit already uses:

- `ExecStartPre=... doctor --strict`

so the daemon will not start if live preflight fails.

Recommended operator checks:

```bash
sudo systemctl status denotary-db-agent
journalctl -u denotary-db-agent -f
```

## Windows Service

Helper scripts:

- [scripts/install-windows-service.ps1](../scripts/install-windows-service.ps1)
- [scripts/remove-windows-service.ps1](../scripts/remove-windows-service.ps1)
- [scripts/run-windows-service.ps1](../scripts/run-windows-service.ps1)
- starter config pack:
  - [deploy/config/windows-agent.example.json](../deploy/config/windows-agent.example.json)

Install example:

```powershell
./scripts/install-windows-service.ps1 `
  -ConfigPath C:\deNotary\denotary-db-agent\config\agent.json `
  -PythonExe C:\Python39\python.exe `
  -ServiceName deNotaryDbAgent `
  -DoctorSource pg-core-ledger `
  -IntervalSec 5

Start-Service deNotaryDbAgent
```

The installer now points the service at `run-windows-service.ps1`, which:

1. runs `doctor --strict`
2. starts daemon mode only if preflight succeeds

Remove example:

```powershell
./scripts/remove-windows-service.ps1 -ServiceName deNotaryDbAgent
```

Recommended operator checks:

```powershell
Get-Service deNotaryDbAgent
```

## Docker Compose

Template:

- [deploy/docker-compose.example.yml](../deploy/docker-compose.example.yml)
- starter config pack:
  - [deploy/config/docker-agent.example.json](../deploy/config/docker-agent.example.json)

Behavior:

- builds the local Dockerfile
- mounts a config JSON
- mounts a persistent Docker volume for local state
- runs daemon mode with `--interval-sec 5`

Typical use:

```bash
docker compose -f deploy/docker-compose.example.yml up -d --build
docker compose -f deploy/docker-compose.example.yml logs -f
```

## Production Notes

- prefer `broadcast_backend = "private_key_env"` with a restricted `env_file`
- keep any inline `submitter_private_key` out of git and reserve it for debug/bootstrap only
- prefer `submitter_permission = "dnanchor"`
- set `proof_retention`, `delivery_retention`, and `dlq_retention`
- enable `diagnostics_snapshot_interval_sec` if you want autonomous operator snapshots
- use `metrics` and `diagnostics` in health checks and runbooks
- run `doctor` before first production enablement, after key rotation, and before moving the agent to a new host
- start from the platform-specific config packs and then replace:
  - watcher token
  - PostgreSQL password
  - `env_file` / `submitter_private_key_env`
  - live `schema_id` / `policy_id`

## Suggested First Operator Checklist

1. Run `validate`
2. Run `doctor`
3. Run `bootstrap`
4. Run `inspect`
5. Start daemon mode
6. Verify `health`
7. Verify `metrics`
8. Verify periodic diagnostics snapshots are being written when enabled

If PostgreSQL maintenance or recovery causes logical runtime artifacts to disappear:

1. Run `inspect`
2. Confirm whether `slot_exists` or `publication_exists` is now false
3. Run `refresh`
4. Re-run `inspect`
5. Only then resume daemon mode

Optional rollout evidence:

```bash
denotary-db-agent --config /etc/denotary-db-agent/agent.json doctor --source pg-core-ledger --save-snapshot
denotary-db-agent --config /etc/denotary-db-agent/agent.json report --source pg-core-ledger --save-snapshot
```

Saved rollout artifacts are indexed automatically in:

- `data/diagnostics/evidence-manifest.json`

That manifest is also bounded by:

- `storage.evidence_manifest_retention`

To inspect them later:

```bash
denotary-db-agent --config /etc/denotary-db-agent/agent.json artifacts --source pg-core-ledger
denotary-db-agent --config /etc/denotary-db-agent/agent.json artifacts --source pg-core-ledger --latest 5
denotary-db-agent --config /etc/denotary-db-agent/agent.json artifacts --source pg-core-ledger --prune-missing
```

Optional startup gate:

```bash
denotary-db-agent --config /etc/denotary-db-agent/agent.json doctor --source pg-core-ledger --strict
```

For Windows services, this gate is already embedded in the wrapper used by `install-windows-service.ps1`.
