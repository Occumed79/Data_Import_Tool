# Occu-Med Data Import Tool

A reusable Data Import Workbench for cleaning, normalizing, profiling, querying, exporting, and loading operational datasets before they enter Occu-Med applications.

The tool is deliberately separate from the Network Map, Vaccine Prescription Generator, and other end-user apps. It is the shared intake layer those tools can consume.

## Current capabilities

- Upload CSV, TSV, XLSX/XLS, JSON/NDJSON, Parquet, and ZIP files
- Pull files from a direct URL
- Register persistent source URLs in Neon for repeatable refreshes
- SHA-256 change detection so unchanged sources are skipped automatically
- Optional raw-source archiving to Uploadcare for changed source files
- Multi-sheet Excel ingestion
- ZIP batch ingestion
- Column profiling: type, nulls, null %, unique values
- Standardize column names
- Trim and normalize blank string values
- Rename and drop columns
- Title-case selected columns
- US phone normalization
- Optional provider-type classification
- Address-key normalization for street/city/state/postal/country fields
- Fuzzy duplicate/entity matching with optional city/state blocking
- U.S. Census geocoding with status and matched-address output
- De-duplicate on selected keys
- Remove fully blank rows
- Required-field validation with failed-row quarantine
- Join two imported sources on one or more keys
- Compare two dataset versions and label Added / Removed / Changed / Unchanged rows
- Save reusable transformation recipes
- Run read-only DuckDB SQL against the current dataset
- Export clean data to CSV, JSON, Parquet, or XLSX
- Optional direct write to Neon/Postgres with chunked writes to reduce peak memory
- Import history
- Automatic snapshot before destructive Neon replace
- Rollback for imports that have snapshots
- Manual refresh of one source or all active sources from the UI
- CLI refresh runner for scheduled jobs: `python scripts/refresh_sources.py`

## Architecture

    CSV / XLSX / ZIP / URL
            |
            v
       Intake loader
            |
            v
      Polars transforms
            |
            +------> DuckDB SQL workbench
            |
            v
      Cleaned DataFrame
            |
            +------> CSV / XLSX / JSON / Parquet
            |
            +------> Neon / Postgres
                        |
                        +--> downstream apps

Registered URL sources now keep refresh state in Neon, including the last content hash, ETag/Last-Modified metadata when available, last check, last change, status, and error. A changed source is transformed with its saved recipe and replaces its target table only after the existing table is snapshotted.

If a registered source has **Archive raw** enabled and `UPLOADCARE_PUBLIC_KEY` is configured, each changed raw file is uploaded to Uploadcare before transformation. The Uploadcare UUID and CDN URL are stored in both source state and import history, preserving source provenance alongside the normalized Neon table.

## Local run

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    streamlit run app.py

Optional Neon persistence:

    export DATABASE_URL='postgresql+psycopg://...'
    streamlit run app.py

## Render deployment

Create a normal Render Web Service manually from this GitHub repository.

- Runtime: Python 3
- Build command: pip install -r requirements.txt
- Start command: streamlit run app.py --server.address 0.0.0.0 --server.port $PORT
- Environment variable: DATABASE_URL = your Neon pooled PostgreSQL connection string
- Optional environment variable: UPLOADCARE_PUBLIC_KEY = Uploadcare public key for raw-source archiving

No database is required just to use the file cleaning and export workbench. Neon enables persistent recipes, registered source URLs, refresh/change tracking, direct table loading, import history, and rollback snapshots.

For scheduled refreshes, create a normal Render Cron Job using the same repository and environment variables:

- Build command: pip install -r requirements.txt
- Start command: python scripts/refresh_sources.py
- Schedule: choose the cadence appropriate for the registered sources
- Environment variable: DATABASE_URL = the same Neon pooled connection string

The cron runner checks every active source, skips unchanged downloads, and refreshes only sources whose bytes changed.

## Tests

    pytest -q

## Next layer

The code is modular so the next additions can include:

- S3/R2 raw archive adapters in addition to Uploadcare
- additional international geocoding adapters
- richer international address normalization
- downloadable validation/error packages
- chunked large-file ingestion
- saved multi-source merge recipes
- automated refresh jobs
