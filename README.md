# DBSync 🔄

[![Go Version](https://img.shields.io/badge/Go-1.20%2B-blue.svg)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE) <!-- Pastikan file LICENSE ada -->
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)

A robust command-line utility for synchronizing schema and data between relational databases. `dbsync` performs a full sync from a source database to a destination database.

---

## Table of Contents
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Security](#-security)
- [Limitations & Considerations](#️-limitations--considerations) <!-- Mengganti nama section -->
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## ✨ Features

### Database Support
Supports synchronization between the following combinations:

| Source      | Target        |
|-------------|---------------|
| MySQL       | MySQL         |
| MySQL       | PostgreSQL    |
| PostgreSQL  | MySQL         |
| PostgreSQL  | PostgreSQL    |
| SQLite      | MySQL         |
| SQLite      | PostgreSQL    |

### Schema Synchronization
Handles table structure synchronization with configurable strategies:

| Strategy     | Description                                                                    | Status         |
|--------------|--------------------------------------------------------------------------------|----------------|
| `drop_create`| **(Default)** Drops and recreates destination tables. **DESTRUCTIVE!**           | Stable         |
| `alter`      | Attempts `ALTER TABLE` for additions/removals. **Complex type changes may fail.** | Experimental   |
| `none`       | No schema changes are applied to the destination.                                | Stable         |

*   Also attempts to synchronize Indexes and Unique/Foreign Key Constraints (WIP/Experimental).

### Data Synchronization
- Efficient data transfer using configurable batch sizes.
- Idempotent operations using `ON CONFLICT UPDATE` (Upsert).
- Handles large tables using primary key-based pagination (single or composite keys).
- Processes tables concurrently using a configurable worker pool.

### Robustness & Observability
| Feature              | Description                                                    |
|----------------------|----------------------------------------------------------------|
| Retry Logic          | Retries failed DB connections and batch operations.            |
| Graceful Shutdown    | Handles `SIGINT`/`SIGTERM` for clean exit.                      |
| Error Handling       | Option to skip failed tables (`SKIP_FAILED_TABLES`).           |
| Structured Logging   | Uses Zap logger, supports JSON output (`ENABLE_JSON_LOGGING`). |
| Prometheus Metrics   | Exposes detailed metrics on `/metrics` (port `METRICS_PORT`).  |
| Health Checks        | Provides `/healthz` (liveness) & `/readyz` (readiness) checks. |
| Profiling            | Optional pprof endpoints via `ENABLE_PPROF`.                   |
| Timeouts             | Configurable per-table timeout (`TABLE_TIMEOUT`).              |

---

## 📥 Installation

**Prerequisites:**
- Go 1.20+ installed ([golang.org](https://golang.org/))
- Network access to both source and destination databases.
- Appropriate database user privileges (see [Security](#-security)).

**Steps:**
```bash
# 1. Clone the repository
git clone https://github.com/arwahdevops/dbsync.git
cd dbsync

# 2. Install dependencies
go mod tidy

# 3. Build the executable
go build -o dbsync .
```
This creates the `dbsync` executable in the current directory.

---

## ⚙️ Configuration

Configuration is managed via environment variables. Create a `.env` file in the project root or set the variables directly in your environment.

**Example `.env` Snippet:** (See `.env.example` for a full template)
```dotenv
# --- Core Settings ---
SYNC_DIRECTION=mysql-to-postgres # Required
SCHEMA_SYNC_STRATEGY=drop_create   # Required (drop_create, alter, none)

# --- Source Database (Required) ---
SRC_DIALECT=mysql
SRC_HOST=127.0.0.1
SRC_PORT=3306
SRC_USER=source_user
SRC_PASSWORD=your_source_password # Use secret management in production!
SRC_DBNAME=source_db
SRC_SSLMODE=disable               # Use 'require' or higher in production!

# --- Destination Database (Required) ---
DST_DIALECT=postgres
DST_HOST=127.0.0.1
DST_PORT=5432
DST_USER=dest_user
DST_PASSWORD=your_dest_password   # Use secret management in production!
DST_DBNAME=dest_db
DST_SSLMODE=disable               # Use 'require' or higher in production!

# --- Optional Performance Tuning ---
BATCH_SIZE=1000
WORKERS=8
CONN_POOL_SIZE=20
TABLE_TIMEOUT=10m

# --- Optional Error Handling ---
SKIP_FAILED_TABLES=false
MAX_RETRIES=3
RETRY_INTERVAL=5s

# --- Optional Observability ---
DEBUG_MODE=false
ENABLE_JSON_LOGGING=false
ENABLE_PPROF=false
METRICS_PORT=9091
```

**Key Environment Variables:**

| Variable               | Default       | Description                                                                 | Required |
|------------------------|---------------|-----------------------------------------------------------------------------|----------|
| `SYNC_DIRECTION`       | -             | Sync direction (e.g., `mysql-to-postgres`)                                  | Yes      |
| `SRC_DIALECT`          | -             | Source DB dialect (`mysql`, `postgres`, `sqlite`)                           | Yes      |
| `SRC_HOST`             | -             | Source DB host                                                              | Yes      |
| `SRC_PORT`             | -             | Source DB port                                                              | Yes      |
| `SRC_USER`             | -             | Source DB username                                                          | Yes      |
| `SRC_PASSWORD`         | -             | Source DB password                                                          | Yes      |
| `SRC_DBNAME`           | -             | Source DB name / file path (SQLite)                                          | Yes      |
| `SRC_SSLMODE`          | `disable`     | Source DB SSL mode (relevant for MySQL/Postgres)                             | No       |
| `DST_DIALECT`          | -             | Destination DB dialect (`mysql`, `postgres`)                                | Yes      |
| `DST_HOST`             | -             | Destination DB host                                                         | Yes      |
| `DST_PORT`             | -             | Destination DB port                                                         | Yes      |
| `DST_USER`             | -             | Destination DB username                                                     | Yes      |
| `DST_PASSWORD`         | -             | Destination DB password                                                     | Yes      |
| `DST_DBNAME`           | -             | Destination DB name                                                         | Yes      |
| `DST_SSLMODE`          | `disable`     | Destination DB SSL mode                                                     | No       |
| `SCHEMA_SYNC_STRATEGY` | `drop_create` | How to handle schema differences (`drop_create`, `alter`, `none`)             | No       |
| `BATCH_SIZE`           | 1000          | Number of rows per data transfer batch                                      | No       |
| `WORKERS`              | 8             | Number of tables to process concurrently                                    | No       |
| `TABLE_TIMEOUT`        | 5m            | Max duration for processing a single table (schema + data + constraints)      | No       |
| `SKIP_FAILED_TABLES`   | false         | Continue syncing other tables if one fails                                  | No       |
| `DISABLE_FK_DURING_SYNC`| false        | (Experimental) Attempt to disable FKs during data load                     | No       |
| `MAX_RETRIES`          | 3             | Max retries for operations like batch inserts                               | No       |
| `RETRY_INTERVAL`       | 5s            | Wait time between retries                                                   | No       |
| `CONN_POOL_SIZE`       | 20            | Max open DB connections per pool                                            | No       |
| `CONN_MAX_LIFETIME`    | 1h            | Max lifetime for DB connections                                             | No       |
| `DEBUG_MODE`           | false         | Enable verbose debug logging and stack traces                               | No       |
| `ENABLE_JSON_LOGGING`  | false         | Output logs in JSON format                                                  | No       |
| `ENABLE_PPROF`         | false         | Enable pprof HTTP endpoints for profiling                                   | No       |
| `METRICS_PORT`         | 9091          | Port for `/metrics`, `/healthz`, `/readyz`, `/debug/pprof/` endpoints | No       |

---

## 🖥 Usage

**Run the Synchronization:**
Ensure your `.env` file is configured correctly in the same directory.
```bash
./dbsync
```
Logs will be printed to standard output/error.

**Check Observability Endpoints (Defaults to port 9091):**
```bash
# Check metrics
curl http://localhost:9091/metrics

# Check liveness (is the process running?)
curl http://localhost:9091/healthz

# Check readiness (are DB connections ok?)
curl http://localhost:9091/readyz

# Access profiling data (if ENABLE_PPROF=true)
# Example: Get CPU profile for 30 seconds
# go tool pprof http://localhost:9091/debug/pprof/profile?seconds=30
# Or access heap info:
# go tool pprof http://localhost:9091/debug/pprof/heap
```

**Stopping:** Press `Ctrl+C` to send `SIGINT` for a graceful shutdown attempt.

---

## 🔒 Security

*   🚨 **Credential Management:** **NEVER commit secrets** (like database passwords) to version control. Use environment variables provided securely, Kubernetes Secrets, or a dedicated secrets management system (like HashiCorp Vault, AWS Secrets Manager, etc.) in production.
*   🔐 **Least Privilege Principle:** Grant the database users configured for `dbsync` only the minimum necessary permissions:
    *   **Source User:** Requires `SELECT` permissions on the tables to be synced and permissions to query `information_schema` (or equivalent schema metadata tables).
    *   **Target User:** Requires permissions for `SELECT`, `INSERT`, `UPDATE`, `DELETE`. Depending on the `SCHEMA_SYNC_STRATEGY`, it also needs `DROP TABLE`, `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`, `ALTER TABLE ADD CONSTRAINT`, `ALTER TABLE DROP CONSTRAINT`.
*   🔒 **Secure Connections:** **Strongly recommended** to use encrypted database connections in production by setting `SRC_SSLMODE` and `DST_SSLMODE` to `require`, `verify-ca`, or `verify-full` (depending on your setup) for PostgreSQL and configuring equivalent TLS settings for MySQL.

---

## ⚠️ Limitations & Considerations

*   **One-Way, Full Sync:** This tool performs a one-way (source -> destination) full data synchronization on each run. It's not suitable for real-time replication or CDC.
*   **Primary Key Requirement:** Efficient data synchronization relies heavily on sortable primary keys (single or composite) for pagination. Tables without primary keys might fail to sync or perform poorly, especially if large and `SCHEMA_SYNC_STRATEGY=none`.
*   **`alter` Strategy:** The `alter` schema strategy is **experimental**. It may not handle all schema differences correctly, especially complex type changes between different database dialects. Use with caution and thorough testing.
*   **Constraint/Index Sync (WIP):** While the tool attempts to sync indexes and constraints, the detection and DDL generation might be incomplete for all types or database versions (especially CHECK constraints and complex indexes). Foreign Keys are typically applied *after* data sync, which could fail if data violates the constraint.
*   **Complex Types:** Mapping for custom types, complex arrays, spatial data, etc., might be limited. Unknown types often fall back to generic text types.
*   **Generated Columns:** Data in generated/computed columns is not directly synced; the destination DB is expected to compute them. Type mapping is skipped for such columns.
*   **Performance:** Sync speed depends heavily on network, disk I/O, database tuning, data volume, chosen `WORKERS`/`BATCH_SIZE`, etc.

---

## 🐛 Troubleshooting

**Common Issues & Solutions:**

| Issue                          | Potential Solution                                                                 |
|--------------------------------|------------------------------------------------------------------------------------|
| Connection Failures            | Verify connection details in `.env`, network connectivity, firewalls, DB user creds. |
| Permission Denied              | Check source/target DB user privileges match requirements.                         |
| Schema DDL Errors              | Check logs for failed DDL. Investigate incompatibility. Consider `SCHEMA_SYNC_STRATEGY=none`. |
| Data Sync Errors (Constraint)| Data violates UNIQUE/FK/CHECK. Ensure target schema matches *or* clean source data. |
| Data Sync Errors (Type)      | Data type mismatch between source/target not handled by mapping. Investigate data. |
| Slow Performance               | Tune `WORKERS`, `BATCH_SIZE`, `CONN_POOL_SIZE`. Monitor DB/network performance.    |
| Pagination Issues (Rare)       | Ensure PKs are stable and sortable correctly by the source DB.                     |

**Debugging:**
Set `DEBUG_MODE=true` in your `.env` file for more verbose logging, including SQL queries (passwords redacted) and stack traces on errors. Use `ENABLE_JSON_LOGGING=true` for easier log parsing.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

1.  Fork the repository (`https://github.com/arwahdevops/dbsync/fork`)
2.  Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3.  Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4.  Push to the Branch (`git push origin feature/AmazingFeature`)
5.  Open a Pull Request

---

## 📄 License

Distributed under the MIT License. See [LICENSE](LICENSE) file for more information.
