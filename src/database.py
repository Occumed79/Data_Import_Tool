from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any

import polars as pl
from sqlalchemy import create_engine, inspect, text


def database_url() -> str | None:
    return os.getenv("DATABASE_URL") or os.getenv("NEON_DATABASE_URL")


def database_available() -> bool:
    return bool(database_url())


def get_engine():
    url = database_url()
    if not url:
        raise RuntimeError("DATABASE_URL is not configured.")
    return create_engine(url, pool_pre_ping=True)


def safe_identifier(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip()).strip("_").lower()
    if not cleaned:
        raise ValueError("Table name is empty.")
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned[:60]


def ensure_metadata_tables(engine=None) -> None:
    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dit_recipes (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                config JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dit_import_history (
                id BIGSERIAL PRIMARY KEY,
                source_name TEXT,
                target_table TEXT NOT NULL,
                row_count BIGINT NOT NULL,
                mode TEXT NOT NULL,
                snapshot_table TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))


def save_recipe(name: str, config: dict[str, Any]) -> None:
    engine = get_engine()
    ensure_metadata_tables(engine)
    payload = json.dumps(config)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dit_recipes (name, config)
                VALUES (:name, CAST(:config AS JSONB))
                ON CONFLICT (name)
                DO UPDATE SET config = EXCLUDED.config, updated_at = NOW()
            """),
            {"name": name.strip(), "config": payload},
        )


def list_recipes() -> list[dict[str, Any]]:
    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT id, name, config, updated_at FROM dit_recipes ORDER BY updated_at DESC")
        ).mappings().all()
    return [dict(row) for row in rows]


def list_history(limit: int = 100) -> list[dict[str, Any]]:
    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, source_name, target_table, row_count, mode, snapshot_table, created_at
                FROM dit_import_history
                ORDER BY id DESC
                LIMIT :limit
            """),
            {"limit": limit},
        ).mappings().all()
    return [dict(row) for row in rows]


def write_frame(
    df: pl.DataFrame,
    table_name: str,
    source_name: str = "",
    mode: str = "append",
) -> dict[str, Any]:
    if mode not in {"append", "replace"}:
        raise ValueError("mode must be append or replace")

    table = safe_identifier(table_name)
    engine = get_engine()
    ensure_metadata_tables(engine)
    snapshot_table = None

    with engine.begin() as conn:
        exists = inspect(conn).has_table(table)

        if mode == "replace" and exists:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            snapshot_table = safe_identifier(f"_dit_snapshot_{table}_{stamp}")
            conn.execute(text(f'CREATE TABLE "{snapshot_table}" AS TABLE "{table}"'))

        df.to_pandas().to_sql(
            table,
            con=conn,
            if_exists=mode,
            index=False,
            method="multi",
            chunksize=1000,
        )

        import_id = conn.execute(
            text("""
                INSERT INTO dit_import_history
                    (source_name, target_table, row_count, mode, snapshot_table)
                VALUES
                    (:source_name, :target_table, :row_count, :mode, :snapshot_table)
                RETURNING id
            """),
            {
                "source_name": source_name,
                "target_table": table,
                "row_count": df.height,
                "mode": mode,
                "snapshot_table": snapshot_table,
            },
        ).scalar_one()

    return {
        "import_id": import_id,
        "table": table,
        "rows": df.height,
        "snapshot_table": snapshot_table,
    }


def rollback_import(import_id: int) -> dict[str, Any]:
    engine = get_engine()
    ensure_metadata_tables(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT id, target_table, snapshot_table
                FROM dit_import_history
                WHERE id = :id
            """),
            {"id": int(import_id)},
        ).mappings().first()

        if not row:
            raise ValueError("Import history record not found.")
        if not row["snapshot_table"]:
            raise ValueError("This import has no snapshot to restore.")

        target = safe_identifier(row["target_table"])
        snapshot = safe_identifier(row["snapshot_table"])

        if not inspect(conn).has_table(snapshot):
            raise ValueError("The snapshot table no longer exists.")

        conn.execute(text(f'DROP TABLE IF EXISTS "{target}"'))
        conn.execute(text(f'ALTER TABLE "{snapshot}" RENAME TO "{target}"'))

    return {"restored_table": target, "from_snapshot": snapshot}
