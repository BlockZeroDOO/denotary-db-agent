# Supported Database Targets

## Wave 1 Targets

| Adapter | Target path | Minimum version | CDC path | Current scaffold status |
| --- | --- | --- | --- | --- |
| PostgreSQL | enterprise | 14 | watermark polling baseline now; logical decoding / WAL next | live baseline with signing, finality, and proof export |
| MySQL | enterprise | 8.0 | watermark polling baseline plus shared row-based binlog CDC baseline | live watermark snapshot baseline with local full-cycle proof export; unit-covered binlog CDC path |
| MariaDB | enterprise | 10.6 | watermark polling baseline plus shared row-based binlog CDC baseline | live watermark snapshot baseline with local full-cycle proof export; unit-covered shared binlog CDC path |
| SQL Server | enterprise | 2019 | watermark polling baseline now; CDC / Change Tracking next | live Docker-backed watermark snapshot baseline with local full-cycle proof export |
| Oracle | enterprise | 19c | watermark polling baseline now; redo / LogMiner next | live Docker-backed watermark snapshot baseline with local full-cycle proof export |
| MongoDB | enterprise | 6.0 | watermark polling baseline and change streams | live Docker-backed watermark snapshot baseline plus change-stream CDC with local full-cycle proof export |

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
