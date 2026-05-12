# MoltFlow Initialization Plan

## Background & Motivation
While the data engineering world is filled with heavy orchestrators like Airflow, Prefect, and Dagster, MoltFlow is built for a fundamentally different paradigm: **The AI-First Future**. 

MoltFlow is a lightweight, local ETL orchestrator written in Go, using SQLite as its state backend. Its entire architecture is designed around the **LLM Developer Experience (DX)**. Instead of complex Python DAGs that are hard for LLMs to generate reliably, MoltFlow uses strict, declarative YAML configurations. This makes it trivial for an AI agent to architect, generate, and debug data pipelines. 

## Core Differentiators (The "Killer Features")
To stand out from the crowd, MoltFlow focuses on two paradigm-shifting capabilities:
1. **AI-First DX & Standardized YAML:** MoltFlow is built on the premise that humans shouldn't write boilerplate orchestration code. The YAML schema is the core product feature, serving as a perfect communication layer between LLMs (which write the logic) and the Go engine (which executes it at high speed). 
2. **Git-for-Data Branching & Replay:** Because every block's exact evaluated input configuration and output metadata are stored immutably in the SQLite state database, MoltFlow treats pipeline state like a Git repository. Users (via the UI or CLI) can "branch" a historical pipeline state, ask an LLM to tweak a Python or SQL script, and replay just that block to compare the data diffs without re-running the entire pipeline.

## Scope & Impact
This plan covers the initial Go project setup, defining the core block execution architecture, defining the YAML schema, setting up the SQLite metadata store, and creating the YAML parser. It defines a DB-backed state mechanism for idempotency, the architecture for executing custom scripts, large-scale data ingestion, the CLI interface, secrets management, and the foundation for Git-like data branching.

## Proposed Solution
1. **Go Module:** Initialize `github.com/Moltak1/MoltFlow`.
2. **YAML Configuration Schema:**
   The pipeline will be defined in a YAML file executed sequentially (top-down).
   ```yaml
   name: Daily Sales Ingestion
   description: Downloads sales data from S3, ingests it, and runs aggregations.
   schedule: "0 2 * * *" # Cron syntax or similar frequency definition
   blocks:
     - id: download_sales_csv
       type: download_s3
       config:
         bucket: my-sales-bucket
         key: data/daily.csv
         destination: /tmp/sales_{{ run.id }}.csv
         access_key: "{{ env.AWS_ACCESS_KEY_ID }}"
         secret_key: "{{ env.AWS_SECRET_ACCESS_KEY }}"
     - id: ingest_sales
       type: ingest_delimited
       config:
         source: "{{ blocks.download_sales_csv.destination }}"
         target_table: raw_sales
         target_db: sqlite://./data/warehouse.db 
         separator: "|||" 
         write_mode: append 
     - id: python_transform
       type: python_script
       config:
         script_path: ./scripts/transform.py
         args: ["{{ blocks.download_sales_csv.destination }}"]
   ```
3. **Architecture - Core Engine & DB-Backed State:**
   - **Database as Single Source of Truth:** All block outputs are persisted to SQLite.
   - **Concurrency Locking:** The internal SQLite state DB enforces mutual exclusion per `job_id`.
   - **State Injection (Templating via DB & Env):** Expressions like `{{ blocks.<block_id>.<key> }}` and `{{ env.<KEY> }}` are resolved before execution.
4. **Architecture - Script Execution:**
   - **SQL Scripts (`sql_script`):** Executed directly.
   - **Bash Scripts (`bash_script`):** Executed directly via Go's `os/exec`.
   - **Python Scripts (`python_script`):** Managed via isolated virtual environments (`.moltflow/venvs/...`). Output captured via `MOLTFLOW_OUT_FILE`.
5. **Architecture - Data Ingestion & Staging DB:**
   - **Default Staging DB:** Internal SQLite "Staging Database".
   - **Data Typing Strategy (All-Text Staging):** Every column is read and stored purely as `TEXT`.
   - **Large File Ingestion (Memory Safe):** Streaming multi-character separator parser.
   - **Dual-Strategy Database Inserts:** `COPY FROM STDIN` for Postgres, chunked prepared transactions for others.
6. **Architecture - CLI & UI Integration:**
   - Command: `moltflow run <pipeline.yaml>`
   - Flags: `--step <block_id>` for targeted execution (crucial for the Branching/Replay feature), `--mode <execution_mode>` (e.g., `normal`, `setup`).
7. **Architecture - Secrets Management (.env):**
   - MoltFlow uses `godotenv` to load `.env` files automatically, resolving `{{ env.VAR_NAME }}` securely.
8. **Architecture - State Store (SQLite):**
   - Store execution metadata (`runs` and `block_runs` tables).
   - Store job locks (`locks` table).
9. **Directory Structure:**
   - `cmd/moltflow/`: Application entry point.
   - `internal/engine/`: Core execution loop, resumption, DB-backed/Env templating, lock management, branching logic.
   - `internal/blocks/`: Implementations of block types.
   - `internal/config/`: YAML parsing.
   - `internal/store/`: SQLite interactions.
   - `internal/ingest/`: Logic for parsing and chunked transactions.
   - `internal/staging/`: Default internal staging DB logic.
   - `internal/models/`: Shared structs.

## Implementation Steps
1. Run `go mod init github.com/Moltak1/MoltFlow`.
2. Get dependencies (`github.com/mattn/go-sqlite3`, `github.com/lib/pq`, `gopkg.in/yaml.v3`, `github.com/spf13/cobra`, `github.com/joho/godotenv`).
3. Create `internal/models/models.go`.
4. Create `internal/store/db.go`.
5. Create `internal/staging/db.go`.
6. Create `internal/blocks/block.go`.
7. Create `internal/ingest/ingest.go`.
8. Create script block implementations.
9. Create `internal/config/parser.go`.
10. Create `internal/engine/templater.go`.
11. Create `internal/engine/runner.go` (implementing `--step` execution to support Git-like replay).
12. Create `cmd/moltflow/main.go`.
13. Add a sample `pipeline.yaml`, a `.env.example`, and testing scripts.

## Verification
- `go build ./...` succeeds.
- Running `moltflow run pipeline.yaml` works.
- Replaying a specific step using `moltflow run pipeline.yaml --step <id>` successfully queries the historical inputs from SQLite and re-executes only that atomic unit.
