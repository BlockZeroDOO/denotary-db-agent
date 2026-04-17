# deNotary DB Agent Deployment Guide

This guide describes the supported production packaging patterns for `denotary-db-agent`.

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

Typical install flow:

```bash
python -m venv /opt/denotary-db-agent/.venv
/opt/denotary-db-agent/.venv/bin/pip install /opt/denotary-db-agent
sudo cp deploy/systemd/denotary-db-agent.service.example /etc/systemd/system/denotary-db-agent.service
sudo systemctl daemon-reload
sudo systemctl enable denotary-db-agent
sudo systemctl start denotary-db-agent
```

Recommended operator checks:

```bash
sudo systemctl status denotary-db-agent
journalctl -u denotary-db-agent -f
```

## Windows Service

Helper scripts:

- [scripts/install-windows-service.ps1](../scripts/install-windows-service.ps1)
- [scripts/remove-windows-service.ps1](../scripts/remove-windows-service.ps1)

Install example:

```powershell
./scripts/install-windows-service.ps1 `
  -ConfigPath C:\deNotary\denotary-db-agent\config\agent.json `
  -PythonExe C:\Python39\python.exe `
  -ServiceName deNotaryDbAgent `
  -IntervalSec 5

Start-Service deNotaryDbAgent
```

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

- keep `submitter_private_key` out of git
- prefer `submitter_permission = "dnanchor"`
- set `proof_retention`, `delivery_retention`, and `dlq_retention`
- enable `diagnostics_snapshot_interval_sec` if you want autonomous operator snapshots
- use `metrics` and `diagnostics` in health checks and runbooks

## Suggested First Operator Checklist

1. Run `validate`
2. Run `bootstrap`
3. Run `inspect`
4. Start daemon mode
5. Verify `health`
6. Verify `metrics`
7. Verify periodic diagnostics snapshots are being written when enabled
