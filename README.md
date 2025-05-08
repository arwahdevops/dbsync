---
# DBSync 🔄

[![Go Version](https://img.shields.io/badge/Go-1.20%2B-blue.svg)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)

A robust command-line utility for synchronizing schema and data between relational databases. `dbsync` performs a full sync from a source database to a destination database.

---

## Table of Contents
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#-configuration)
  - [Basic Configuration](#basic-configuration)
  - [Optional: Secret Management (Vault)](#optional-secret-management-vault)
  - [Optional: Custom Type Mapping](#optional-custom-type-mapping)
  - [Key Environment Variables](#key-environment-variables)
  - [Command-Line Overrides](#command-line-overrides)
- [Usage](#-usage)
- [Security](#-security)
- [Limitations & Considerations](#️-limitations--considerations)
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

| Strategy     | Description                                                                                                                               | Status         |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------------|
| `drop_create`| **(Default)** Drops and recreates destination tables. **DESTRUCTIVE!**                                                                  | Stable         |
| `alter`      | Attempts `ALTER TABLE` for additions/removals/modifications of columns, indexes, & constraints. **Complex type changes may fail.**       | Experimental   |
| `none`       | No schema changes are applied to the destination.                                                                                        | Stable         |

*   Also attempts to synchronize Indexes and Unique/Foreign Key Constraints. Detection and DDL generation might be limited for complex constraints/indexes (e.g., CHECK, functional indexes).
*   **The `alter` strategy is severely limited for SQLite** due to SQLite's native `ALTER TABLE` limitations (see [Limitations](#️-limitations--considerations)).

### Data Synchronization
- Efficient data transfer using configurable batch sizes.
- Idempotent operations using `ON CONFLICT UPDATE` (Upsert) on supported dialects (PG, MySQL >= 5.7?, SQLite >= 3.24.0).
- Handles large tables using primary key-based pagination (single or composite keys).
- Processes tables concurrently using a configurable worker pool.
- Option (experimental) to disable foreign key constraints (`DISABLE_FK_DURING_SYNC`) during data load for potential speed improvements (risk of invalid data if source is inconsistent).

### Custom Type Mapping
- Allows defining custom data type mappings between source and target dialects via a JSON configuration file (`TYPE_MAPPING_FILE_PATH`).
- **Mapping Process:**
    1.  The source data type is normalized (e.g., `varchar(255)` -> `varchar`, `int(11) unsigned` -> `int`).
    2.  **`special_mappings`:** The JSON file can contain regex patterns (`source_type_pattern`) matched against the *full* original source type name (lowercase, e.g., `tinyint(1)`). If a pattern matches, the specified `target_type` is used. These take priority.
    3.  **`mappings`:** If no special pattern matches, the normalized source type key (from step 1) is looked up in the `mappings` map. If found, the corresponding `target_type` is used.
    4.  **Internal/Fallback:** If no match is found in the JSON configuration, or if the file isn't provided, dbsync might have internal fallbacks or use the source type directly if source and target dialects are the same. If no mapping is found, a generic fallback type (e.g., `TEXT`) is used.
    5.  **Modifiers:** After the base target type is determined (from step 2, 3, or 4), `dbsync` attempts to apply modifiers (length, precision, scale) from the original source type to the target base type, if relevant for that target data type.

### Robustness & Observability
| Feature              | Description                                                                  |
|----------------------|----------------------------------------------------------------------------|
| Retry Logic          | Retries failed DB connections and batch operations.                          |
| Graceful Shutdown    | Handles `SIGINT`/`SIGTERM` for clean exit.                                  |
| Error Handling       | Option to skip failed tables (`SKIP_FAILED_TABLES`).                         |
| Structured Logging   | Uses Zap logger, supports JSON output (`ENABLE_JSON_LOGGING`).             |
| Prometheus Metrics   | Exposes detailed metrics on `/metrics` (port `METRICS_PORT`).               |
| Health Checks        | Provides `/healthz` (liveness) & `/readyz` (readiness) endpoints.          |
| Profiling            | Optional pprof endpoints via `ENABLE_PPROF`.                               |
| Timeouts             | Configurable per-table timeout (`TABLE_TIMEOUT`).                           |
| Secret Management    | Optional integration with HashiCorp Vault.                                 |

---

## 📥 Installation

**Prerequisites:**
- Go 1.20+ installed ([golang.org](https://golang.org/))
- Network access to both source and destination databases.
- Appropriate database user privileges (see [Security](#-security)).
- (Optional) Access to a configured HashiCorp Vault instance if using secret management.

**Steps:**
```bash
# 1. Clone the repository
git clone https://github.com/arwahdevops/dbsync.git
cd dbsync

# 2. Install dependencies (including Vault client if needed)
go mod tidy

# 3. Build the executable
go build -o dbsync .
```
This creates the `dbsync` executable in the current directory.

---

## ⚙️ Configuration

Configuration is managed via environment variables. Create a `.env` file in the project root or set the variables directly in your environment. You can also override some settings via command-line flags.

### Basic Configuration

At minimum, you need to define the sync direction and connection details for both databases.

**Example `.env` Snippet (Direct Password):**
```dotenv
# --- Core Settings ---
SYNC_DIRECTION=mysql-to-postgres # Required: mysql-to-mysql, mysql-to-postgres, postgres-to-mysql, postgres-to-postgres, sqlite-to-mysql, sqlite-to-postgres
SCHEMA_SYNC_STRATEGY=drop_create   # drop_create, alter, none

# --- Source Database (Required) ---
SRC_DIALECT=mysql             # mysql, postgres, sqlite
SRC_HOST=127.0.0.1
SRC_PORT=3306
SRC_USER=source_user          # Username is always required
SRC_PASSWORD=your_source_password # Provide password here if NOT using Vault
SRC_DBNAME=source_db          # DB Name / File path for SQLite
SRC_SSLMODE=disable           # disable, require, verify-ca, verify-full (adjust per DB)

# --- Destination Database (Required) ---
DST_DIALECT=postgres          # mysql, postgres
DST_HOST=127.0.0.1
DST_PORT=5432
DST_USER=dest_user            # Username is always required
DST_PASSWORD=your_dest_password # Provide password here if NOT using Vault
DST_DBNAME=dest_db
DST_SSLMODE=disable           # disable, require, verify-ca, verify-full (adjust per DB)

# --- Other Optional Settings ---
# BATCH_SIZE=1000
# WORKERS=8
# TABLE_TIMEOUT=5m
# SKIP_FAILED_TABLES=false
# DISABLE_FK_DURING_SYNC=false # Set to true to attempt FK disabling during data load (experimental)
# DEBUG_MODE=false
# ENABLE_JSON_LOGGING=false
# ENABLE_PPROF=false
# METRICS_PORT=9091
# TYPE_MAPPING_FILE_PATH=./typemap.json
# ... etc ...
```

### Optional: Secret Management (Vault)

If you prefer to fetch database passwords from HashiCorp Vault:

1.  **Leave `SRC_PASSWORD` and/or `DST_PASSWORD` empty or unset** in your environment/`.env` file.
2.  **Enable and configure Vault** using the following variables:

**Example `.env` Snippet (Using Vault):**
```dotenv
# --- Core Settings ---
SYNC_DIRECTION=mysql-to-postgres
SCHEMA_SYNC_STRATEGY=drop_create

# --- Source Database (Username still required) ---
SRC_DIALECT=mysql
SRC_HOST=127.0.0.1
SRC_PORT=3306
SRC_USER=source_user
# SRC_PASSWORD= # Leave empty or omit to use Vault
SRC_DBNAME=source_db
SRC_SSLMODE=disable

# --- Destination Database (Username still required) ---
DST_DIALECT=postgres
DST_HOST=127.0.0.1
DST_PORT=5432
DST_USER=dest_user
# DST_PASSWORD= # Leave empty or omit to use Vault
DST_DBNAME=dest_db
DST_SSLMODE=disable

# --- Vault Secret Management (Required if Password vars are empty) ---
VAULT_ENABLED=true                # Enable Vault integration
VAULT_ADDR=https://your-vault.example.com:8200 # Vault server address
VAULT_TOKEN=hvs.YOUR_VAULT_TOKEN  # Vault token for authentication (or configure other methods)
# VAULT_CACERT=/path/to/ca.crt    # Optional: Path to CA cert for Vault TLS
# VAULT_SKIP_VERIFY=false         # Optional: Skip TLS verification (INSECURE)
SRC_SECRET_PATH=secret/data/dbsync/source_db # Path to source secret (KV v2)
DST_SECRET_PATH=secret/data/dbsync/dest_db   # Path to destination secret (KV v2)
# Optional: Keys within the secret if not 'username'/'password'
# SRC_USERNAME_KEY=db_user
# SRC_PASSWORD_KEY=the_password
# DST_USERNAME_KEY=user
# DST_PASSWORD_KEY=pass
```

**Priority:** If `SRC_PASSWORD`/`DST_PASSWORD` environment variables are set, they will be used **instead** of fetching from Vault, even if `VAULT_ENABLED=true`.

### Optional: Custom Type Mapping

You can provide a JSON file to define custom data type mappings between source and target dialects. This is useful for handling specific data types or for overriding default mapping behavior.

**Example `typemap.json`:**
```json
{
  "type_mappings": [
    {
      "source_dialect": "mysql",
      "target_dialect": "postgres",
      "mappings": {
        "int": "INTEGER",         // Mapping from normalized source type
        "varchar": "VARCHAR",
        "text": "TEXT"
        // ... other standard mappings ...
      },
      "special_mappings": [
        {
          // Match original source type (lowercase) with regex
          "source_type_pattern": "^tinyint\\(1\\)$",
          "target_type": "BOOLEAN" // Target type if match
        },
        {
          "source_type_pattern": "^enum\\((.*)\\)$", // Capture enum values (currently unused)
          "target_type": "VARCHAR(255)" // Map all enums to VARCHAR
        }
      ]
    }
    // ... other mapping configurations between dialects ...
  ]
}
```
*   `mappings`: Direct mapping from the (basic normalized) source type name (e.g., "int" not "int(11)") to the target base type name.
*   `special_mappings`:
    *   `source_type_pattern`: A regex pattern matched against the full lowercase source type name (e.g., "tinyint(1)", "varchar(255)").
    *   `target_type`: The target base type to use if the pattern matches.
*   **Modifiers (Length/Precision/Scale):** After the target base type is determined from `mappings` or `special_mappings`, `dbsync` attempts to apply modifiers from the original source type to the target type if relevant.

Set the `TYPE_MAPPING_FILE_PATH` environment variable to point to this file (e.g., `TYPE_MAPPING_FILE_PATH=./typemap.json`). If not configured or the file is not found, `dbsync` will use internal fallbacks or use types directly if dialects are the same or no mapping is found.

### Key Environment Variables

| Variable               | Default       | Description                                                                          | Required |
|------------------------|---------------|------------------------------------------------------------------------------------|----------|
| `SYNC_DIRECTION`       | -             | Sync direction (e.g., `mysql-to-postgres`)                                         | Yes      |
| `SRC_DIALECT`          | -             | Source DB dialect (`mysql`, `postgres`, `sqlite`)                                | Yes      |
| `SRC_HOST`             | -             | Source DB host                                                                   | Yes      |
| `SRC_PORT`             | -             | Source DB port                                                                   | Yes      |
| `SRC_USER`             | -             | Source DB username (Always required)                                             | Yes      |
| `SRC_PASSWORD`         | *(empty)*     | Source DB password (Use this OR Vault)                                             | Optional |
| `SRC_DBNAME`           | -             | Source DB name / file path (SQLite)                                                | Yes      |
| `SRC_SSLMODE`          | `disable`     | Source DB SSL mode (relevant for MySQL/Postgres)                                   | No       |
| `DST_DIALECT`          | -             | Destination DB dialect (`mysql`, `postgres`)                                       | Yes      |
| `DST_HOST`             | -             | Destination DB host                                                                | Yes      |
| `DST_PORT`             | -             | Destination DB port                                                                | Yes      |
| `DST_USER`             | -             | Destination DB username (Always required)                                        | Yes      |
| `DST_PASSWORD`         | *(empty)*     | Destination DB password (Use this OR Vault)                                        | Optional |
| `DST_DBNAME`           | -             | Destination DB name                                                                | Yes      |
| `DST_SSLMODE`          | `disable`     | Destination DB SSL mode                                                            | No       |
| `SCHEMA_SYNC_STRATEGY` | `drop_create` | How to handle schema differences (`drop_create`, `alter`, `none`)                | No       |
| `BATCH_SIZE`           | 1000          | Number of rows per data transfer batch                                             | No       |
| `WORKERS`              | 8             | Number of tables to process concurrently                                         | No       |
| `TABLE_TIMEOUT`        | 5m            | Max duration for processing a single table (schema + data + constraints)           | No       |
| `SKIP_FAILED_TABLES`   | false         | Continue syncing other tables if one fails                                         | No       |
| `DISABLE_FK_DURING_SYNC`| false        | (Experimental) Attempt to disable FKs during data load                          | No       |
| `MAX_RETRIES`          | 3             | Max retries for operations like batch inserts                                      | No       |
| `RETRY_INTERVAL`       | 5s            | Wait time between retries                                                          | No       |
| `CONN_POOL_SIZE`       | 20            | Max open DB connections per pool                                                   | No       |
| `CONN_MAX_LIFETIME`    | 1h            | Max lifetime for DB connections                                                    | No       |
| `DEBUG_MODE`           | false         | Enable verbose debug logging and stack traces                                    | No       |
| `ENABLE_JSON_LOGGING`  | false         | Output logs in JSON format                                                         | No       |
| `ENABLE_PPROF`         | false         | Enable pprof HTTP endpoints for profiling                                         | No       |
| `METRICS_PORT`         | 9091          | Port for `/metrics`, `/healthz`, `/readyz`, `/debug/pprof/` endpoints           | No       |
| `TYPE_MAPPING_FILE_PATH`| *(empty)*     | Path to custom type mapping JSON file (optional, default `./typemap.json`)         | No       |
| **Vault Specific**     |               |                                                                                    |          |
| `VAULT_ENABLED`        | `false`       | Set `true` to enable Vault integration                                             | No       |
| `VAULT_ADDR`           | `https://...` | Vault server address                                                               | If Enabled |
| `VAULT_TOKEN`          | -             | Vault token for authentication                                                     | If Enabled (and using token auth) |
| `VAULT_CACERT`         | -             | Path to Vault CA certificate (optional)                                            | No       |
| `VAULT_SKIP_VERIFY`    | `false`       | Skip Vault TLS verification (INSECURE)                                             | No       |
| `SRC_SECRET_PATH`      | -             | Path to source DB secret in Vault (KV v2)                                          | If Enabled |
| `DST_SECRET_PATH`      | -             | Path to destination DB secret in Vault (KV v2)                                     | If Enabled |
| `SRC_USERNAME_KEY`     | `username`    | Key name for username within the source Vault secret data                          | No       |
| `SRC_PASSWORD_KEY`     | `password`    | Key name for password within the source Vault secret data                          | No       |
| `DST_USERNAME_KEY`     | `username`    | Key name for username within the destination Vault secret data                     | No       |
| `DST_PASSWORD_KEY`     | `password`    | Key name for password within the destination Vault secret data                     | No       |

*(See `.env.example` for the full list and descriptions)*

### Command-Line Overrides

Some configuration settings from environment variables can be overridden using command-line flags:

*   `-sync-direction`: Overrides `SYNC_DIRECTION` (e.g., `mysql-to-postgres`).
*   `-batch-size`: Overrides `BATCH_SIZE` (must be > 0).
*   `-workers`: Overrides `WORKERS` (must be > 0).
*   `-schema-strategy`: Overrides `SCHEMA_SYNC_STRATEGY` (`drop_create`, `alter`, `none`).
*   `-type-map-file`: Overrides `TYPE_MAPPING_FILE_PATH`.

**Example:**
```bash
./dbsync -sync-direction sqlite-to-mysql -batch-size 500 -type-map-file /etc/dbsync/my_typemap.json
```
CLI flags will take precedence over their corresponding environment variables.

---

## 🖥 Usage

**Run the Synchronization:**
Ensure your `.env` file is configured or environment variables are set.
```bash
./dbsync
```
Or with flag overrides:
```bash
./dbsync -schema-strategy alter -workers 4
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

*   🚨 **Credential Management:** **NEVER commit secrets** (like database passwords or Vault tokens) to version control.
    *   For production, use environment variables provided securely by your deployment system, Kubernetes Secrets, or integrate with a dedicated secrets management system (HashiCorp Vault is supported optionally).
    *   If using direct password variables (`SRC_PASSWORD`, `DST_PASSWORD`), ensure they are handled securely.
*   🔐 **Least Privilege Principle:** Grant the database users configured for `dbsync` only the minimum necessary permissions:
    *   **Source User:** Requires `SELECT` permissions on the tables to be synced and permissions to query `information_schema` (or equivalent schema metadata tables).
    *   **Target User:** Requires permissions for `SELECT`, `INSERT`, `UPDATE`, `DELETE`. Depending on the `SCHEMA_SYNC_STRATEGY`, it also needs `DROP TABLE`, `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`, `ALTER TABLE ADD CONSTRAINT`, `ALTER TABLE DROP CONSTRAINT`.
*   🔒 **Secure Connections:** **Strongly recommended** to use encrypted database connections in production by setting `SRC_SSLMODE` and `DST_SSLMODE` to `require`, `verify-ca`, or `verify-full` (depending on your setup) for PostgreSQL and configuring equivalent TLS settings for MySQL. Similarly, ensure the connection to Vault (`VAULT_ADDR`) uses HTTPS and configure `VAULT_CACERT` or do not set `VAULT_SKIP_VERIFY=true`.

---

## ⚠️ Limitations & Considerations

*   **One-Way, Full Sync:** This tool performs a one-way (source -> destination) full data synchronization on each run. It's not suitable for real-time replication or Change Data Capture (CDC).
*   **Primary Key Requirement:** Efficient data synchronization relies heavily on sortable primary keys (single or composite) for pagination. Tables without primary keys might fail to sync data reliably or perform poorly, especially if large.
*   **`alter` Strategy (Experimental):**
    *   The `alter` schema strategy is tagged **experimental**. Its ability to handle all schema differences may be limited, particularly:
        *   **Complex Type Changes:** Changing data types between different dialects (e.g., `TEXT` to `VARCHAR(10)` in MySQL, `VARCHAR` to `INT`) might fail, cause data loss/truncation, or require manual `USING` clauses (PostgreSQL) that `dbsync` may not generate perfectly. Use with extreme caution for type changes.
        *   **Modifying `IDENTITY`/`AUTO_INCREMENT`:** Adding or removing these properties on existing columns is usually not handled automatically by the `alter` strategy.
        *   **Generated Columns:** Changes to generated column expressions are not detected or handled.
    *   The `drop_create` strategy is more reliable for significant schema changes.
*   **SQLite `ALTER TABLE` Limitations:** SQLite has very limited `ALTER TABLE` support. The `alter` strategy **will not** attempt to change the type, nullability, default value, or collation of existing columns in SQLite. For such changes, you must recreate the table manually or use the `drop_create` strategy.
*   **Constraint/Index Sync:** While the tool attempts to sync indexes and constraints, detection and DDL generation might be incomplete for all types (especially `CHECK` constraints) or database versions. Foreign Keys are typically applied *after* data sync, which could fail if data violates the constraint. `DISABLE_FK_DURING_SYNC` can help initial load but doesn't fix underlying data issues.
*   **Complex Data Types:** Mapping for custom types, complex arrays, spatial data, etc., might be limited or require explicit definitions in `typemap.json`. Unknown types often fallback to generic text types.
*   **Generated/Computed Columns:** Data in generated/computed columns is not directly synced; the destination DB is expected to compute them based on its definition. Type mapping is skipped for such columns.
*   **Performance:** Sync speed depends heavily on network latency/bandwidth, disk I/O, database tuning, data volume, chosen `WORKERS`/`BATCH_SIZE`, complexity of transformations, etc.

---

## 🐛 Troubleshooting

**Common Issues & Solutions:**

| Issue                          | Potential Solution                                                                          |
|---------------------------------|-------------------------------------------------------------------------------------------|
| Connection Failures            | Verify DB connection details (`.env`/flags), network connectivity, firewalls, DB user credentials & privileges. |
| Vault Connection/Auth Errors   | Verify `VAULT_ADDR`, `VAULT_TOKEN` (or other auth method), network access, TLS config (`VAULT_CACERT`, `VAULT_SKIP_VERIFY`). |
| Secret Not Found (Vault)       | Check `SRC/DST_SECRET_PATH` exists in Vault and the token has permission. Ensure keys (`username`/`password` or custom) exist within the secret's `data` field (KV v2). |
| Permission Denied              | Check source/target DB user privileges match requirements (SELECT on source; SELECT, INSERT, UPDATE, DELETE, CREATE/ALTER/DROP on target depending on strategy). |
| Schema DDL Errors (ALTER/CREATE)| Check logs for the specific failed DDL statement. Investigate incompatibility or syntax errors. Consider `SCHEMA_SYNC_STRATEGY=none` or `drop_create`. Note limitations of `alter` strategy, especially for type changes and SQLite. |
| Data Sync Errors (Constraint)  | Data violates `UNIQUE`/`FK`/`CHECK` constraint on destination. Ensure target schema matches *or* clean source data. `DISABLE_FK_DURING_SYNC=true` might allow loading but doesn't fix the data. |
| Data Sync Errors (Type)        | Data type mismatch between source/target not handled correctly by mapping (e.g., string too long for target `VARCHAR`, non-numeric in numeric column). Investigate source data or adjust `typemap.json`. |
| Invalid Type Mapping File Error| Ensure `TYPE_MAPPING_FILE_PATH` points to a valid JSON file. Check JSON syntax & regex patterns. |
| Slow Performance               | Tune `WORKERS`, `BATCH_SIZE`, `CONN_POOL_SIZE`. Monitor DB/network performance. Analyze query plans on source/destination. |
| Pagination Issues (No PK / Rare)| Ensure PKs exist for large tables if using `alter` or `drop_create`. Check if PK values are stable and sortable correctly by the source DB. |

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


---
