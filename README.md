# Occu-Med Data Import Tool

A reusable Data Import Workbench for cleaning, normalizing, profiling, querying, exporting, and loading operational datasets before they enter Occu-Med applications.

The tool is deliberately separate from the Network Map, Vaccine Prescription Generator, and other end-user apps. It is the shared intake layer those tools can consume.

## Current capabilities

- Upload CSV, TSV, XLSX/XLS, JSON/NDJSON, Parquet, and ZIP files
- Pull files from a direct URL
- Multi-sheet Excel ingestion
- ZIP batch ingestion
- Column profiling: type, nulls, null %, unique values
- Standardize column names
- Trim and normalize blank string values
- Rename and drop columns
- Title-case selected columns
- US phone normalization
- Optional provider-type classification
- De-duplicate on selected keys
- Remove fully blank rows
- Save reusable transformation recipes
- Run read-only DuckDB SQL against the current dataset
- Export clean data to CSV, JSON, Parquet, or XLSX
- Optional direct write to Neon/Postgres
- Import history
- Automatic snapshot before destructive Neon replace
- Rollback for imports that have snapshots

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

The raw-file archive layer can later be connected to Uploadcare, S3, or R2 without changing the transform or database layers.

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

No database is required just to use the file cleaning and export workbench. Neon enables persistent recipes, direct table loading, import history, and rollback snapshots.

## Tests

    pytest -q

## Next layer

The code is modular so the next additions can include:

- URL/source scheduling
- raw file archive integration
- Census/geocoding adapters
- richer address normalization
- exact and fuzzy entity matching
- source-to-source diff reports
- failed-row quarantine and downloadable error files
- chunked large-file ingestion
- multi-source merge recipes
- automated refresh jobs
