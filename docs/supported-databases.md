# Supported Database Targets

## Wave 1 Targets

| Adapter | Target path | Minimum version | CDC path | Current scaffold status |
| --- | --- | --- | --- | --- |
| PostgreSQL | enterprise | 14 | watermark polling baseline now; logical decoding / WAL next | live baseline with signing, finality, and proof export |
| MySQL | enterprise | 8.0 | watermark polling baseline now; row-based binlog next | live watermark snapshot baseline with local full-cycle proof export |
| MariaDB | enterprise | 10.6 | watermark polling baseline now; MariaDB binlog profile next | live watermark snapshot baseline with local full-cycle proof export |
| SQL Server | enterprise | 2019 | watermark polling baseline now; CDC / Change Tracking next | live Docker-backed watermark snapshot baseline with local full-cycle proof export |
| Oracle | enterprise | 19c | redo / LogMiner compatible | capability scaffold |
| MongoDB | enterprise | 6.0 | change streams | capability scaffold |

## Common v1 Rules

- `insert`, `update`, and `delete` are the target operations
- raw row payload remains off-chain
- canonical event hashes and source metadata are notarized
- request preparation uses `Ingress API`
- finality and receipts stay in the existing `deNotary` backend
- for PostgreSQL, MySQL, and MariaDB `single` events, sign/broadcast and proof retrieval already happen inside the plugin

## Explicitly Out Of Scope For v1

- Redis
- SQLite
- ClickHouse
- Elasticsearch / OpenSearch
- Cassandra
- native in-database extensions
