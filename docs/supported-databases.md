# Supported Database Targets

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

## Wave 1 Targets

| Adapter | Target path | Minimum version | CDC path | Current production-validation status |
| --- | --- | --- | --- | --- |
| PostgreSQL | enterprise | 14 | watermark polling, trigger CDC, logical decoding, pgoutput streaming | live local full-cycle, stable Jungle4 path, and real `denotary` mainnet validation |
| MySQL | enterprise | 8.0 | watermark polling baseline plus shared row-based binlog CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| MariaDB | enterprise | 10.6 | watermark polling baseline plus shared row-based binlog CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| SQL Server | enterprise | 2019 | watermark polling baseline plus native Change Tracking CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| Oracle | enterprise | 19c | watermark polling baseline plus LogMiner CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| MongoDB | enterprise | 6.0 | watermark polling baseline and change streams | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |

## Common v1 Rules

- `insert`, `update`, and `delete` are the target operations
- raw row payload remains off-chain
- canonical event hashes and source metadata are notarized
- request preparation uses `Ingress API`
- finality and receipts stay in the existing `deNotary` backend
- for PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, and MongoDB `single` events, sign/broadcast and proof retrieval already happen inside the plugin

## Explicitly Out Of Scope For v1

- Redis
- SQLite
- ClickHouse
- Elasticsearch / OpenSearch
- Cassandra
- native in-database extensions
## Wave 2 Targets

| Adapter | Initial target path | Supported capture path | Current status |
| --- | --- | --- | --- |
| Redis | operational state | explicit key-pattern polling through `SCAN` | polling baseline, local full-cycle validation, Docker-backed live validation, restart, short-soak, long-soak, local service-outage, real `denotary` happy-path, bounded budget, and real mainnet degraded-service validation implemented |
| ScyllaDB | distributed wide-column | Cassandra-compatible snapshot / watermark polling | shared Cassandra-compatible polling baseline, local full-cycle proof export, Docker-backed live validation, restart, short-soak, long-soak, local service-outage, real `denotary` happy-path, bounded budget, and real mainnet degraded-service validation implemented |
| IBM Db2 | enterprise | tracked-table snapshot / watermark polling | tracked-table polling baseline, env-gated and local Docker-backed live validation, local full-cycle proof export, restart, short-soak, long-soak, local service-outage, real `denotary` happy-path, bounded budget, and real mainnet degraded-service validation implemented |
| Apache Cassandra | distributed wide-column | partition-aware snapshot / watermark polling | tracked-table polling baseline, env-gated and local Docker-backed live validation, local full-cycle proof export, restart, short-soak, long-soak, local service-outage, real `denotary` happy-path, bounded budget, and real mainnet degraded-service validation implemented |
| Elasticsearch | search / operational index | query-based snapshot and incremental polling | tracked-index polling baseline, env-gated and local Docker-backed live validation, local full-cycle proof export, restart, short-soak, long-soak, local service-outage, real `denotary` happy-path, bounded budget, and real mainnet degraded-service validation implemented |
| SQLite | embedded / edge | local snapshot / watermark polling | file-backed polling baseline, local full-cycle proof export, cold restart, short-soak, long-soak, local service-outage, real `denotary` happy-path, bounded budget, and real mainnet degraded-service validation implemented |
