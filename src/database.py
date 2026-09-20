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
        conn.execute(text("""
            ALTER TABLE dit_import_history
                ADD COLUMN IF NOT EXISTS source_id BIGINT,
                ADD COLUMN IF NOT EXISTS source_url TEXT,
                ADD COLUMN IF NOT EXISTS source_hash TEXT,
                ADD COLUMN IF NOT EXISTS recipe_name TEXT
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dit_merge_recipes (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                left_dataset TEXT NOT NULL,
                right_dataset TEXT NOT NULL,
                join_keys JSONB NOT NULL,
                join_mode TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dit_sources (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                url TEXT NOT NULL,
                target_table TEXT NOT NULL,
                recipe_name TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                last_hash TEXT,
                last_etag TEXT,
                last_modified TEXT,
                last_status TEXT,
                last_error TEXT,
                last_checked_at TIMESTAMPTZ,
                last_changed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
                SELECT
                    id, source_id, source_name, source_url, source_hash, recipe_name,
                    target_table, row_count, mode, snapshot_table, created_at
                FROM dit_import_history
                ORDER BY id DESC
                LIMIT :limit
            """),
            {"limit": limit},
        ).mappings().all()
    return [dict(row) for row in rows]


def save_merge_recipe(
    name: str,
    left_dataset: str,
    right_dataset: str,
    join_keys: list[str],
    join_mode: str,
) -> None:
    if not join_keys:
        raise ValueError("At least one join key is required.")
    if join_mode not in {"left", "inner", "full"}:
        raise ValueError("Unsupported join mode.")

    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dit_merge_recipes
                    (name, left_dataset, right_dataset, join_keys, join_mode)
                VALUES
                    (:name, :left_dataset, :right_dataset, CAST(:join_keys AS JSONB), :join_mode)
                ON CONFLICT (name)
                DO UPDATE SET
                    left_dataset = EXCLUDED.left_dataset,
                    right_dataset = EXCLUDED.right_dataset,
                    join_keys = EXCLUDED.join_keys,
                    join_mode = EXCLUDED.join_mode,
                    updated_at = NOW()
            """),
            {
                "name": name.strip(),
                "left_dataset": left_dataset,
                "right_dataset": right_dataset,
                "join_keys": json.dumps(join_keys),
                "join_mode": join_mode,
            },
        )


def list_merge_recipes() -> list[dict[str, Any]]:
    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, name, left_dataset, right_dataset, join_keys, join_mode, updated_at
                FROM dit_merge_recipes
                ORDER BY updated_at DESC
            """)
        ).mappings().all()
    return [dict(row) for row in rows]


def save_source(
    name: str,
    url: str,
    target_table: str,
    recipe_name: str | None = None,
    active: bool = True,
) -> None:
    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dit_sources (name, url, target_table, recipe_name, active)
                VALUES (:name, :url, :target_table, :recipe_name, :active)
                ON CONFLICT (name)
                DO UPDATE SET
                    url = EXCLUDED.url,
                    target_table = EXCLUDED.target_table,
                    recipe_name = EXCLUDED.recipe_name,
                    active = EXCLUDED.active,
                    updated_at = NOW()
            """),
            {
                "name": name.strip(),
                "url": url.strip(),
                "target_table": safe_identifier(target_table),
                "recipe_name": recipe_name or None,
                "active": bool(active),
            },
        )


def list_sources(active_only: bool = False) -> list[dict[str, Any]]:
    engine = get_engine()
    ensure_metadata_tables(engine)
    query = """
        SELECT
            id, name, url, target_table, recipe_name, active,
            last_hash, last_etag, last_modified, last_status, last_error,
            last_checked_at, last_changed_at, created_at, updated_at
        FROM dit_sources
    """
    if active_only:
        query += " WHERE active = TRUE"
    query += " ORDER BY name"
    with engine.begin() as conn:
        rows = conn.execute(text(query)).mappings().all()
    return [dict(row) for row in rows]


def get_source(source_id: int) -> dict[str, Any] | None:
    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT
                    id, name, url, target_table, recipe_name, active,
                    last_hash, last_etag, last_modified, last_status, last_error,
                    last_checked_at, last_changed_at, created_at, updated_at
                FROM dit_sources
                WHERE id = :id
            """),
            {"id": int(source_id)},
        ).mappings().first()
    return dict(row) if row else None


def update_source_refresh_state(
    source_id: int,
    status: str,
    source_hash: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    changed: bool = False,
    error: str | None = None,
) -> None:
    engine = get_engine()
    ensure_metadata_tables(engine)
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE dit_sources
                SET
                    last_status = :status,
                    last_hash = COALESCE(:source_hash, last_hash),
                    last_etag = COALESCE(:etag, last_etag),
                    last_modified = COALESCE(:last_modified, last_modified),
                    last_error = :error,
                    last_checked_at = NOW(),
                    last_changed_at = CASE WHEN :changed THEN NOW() ELSE last_changed_at END,
                    updated_at = NOW()
                WHERE id = :id
            """),
            {
                "id": int(source_id),
                "status": status,
                "source_hash": source_hash,
                "etag": etag,
                "last_modified": last_modified,
                "changed": bool(changed),
                "error": error,
            },
        )


def write_frame(
    df: pl.DataFrame,
    table_name: str,
    source_name: str = "",
    mode: str = "append",
    source_id: int | None = None,
    source_url: str | None = None,
    source_hash: str | None = None,
    recipe_name: str | None = None,
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

        chunk_size = 10_000
        first = True
        for chunk in df.iter_slices(n_rows=chunk_size):
            if_exists = mode if first else "append"
            chunk.to_pandas().to_sql(
                table,
                con=conn,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000,
            )
            first = False

        if df.height == 0:
            df.to_pandas().head(0).to_sql(
                table,
                con=conn,
                if_exists=mode,
                index=False,
            )

        import_id = conn.execute(
            text("""
                INSERT INTO dit_import_history
                    (
                        source_id, source_name, source_url, source_hash, recipe_name,
                        target_table, row_count, mode, snapshot_table
                    )
                VALUES
                    (
                        :source_id, :source_name, :source_url, :source_hash, :recipe_name,
                        :target_table, :row_count, :mode, :snapshot_table
                    )
                RETURNING id
            """),
            {
                "source_id": source_id,
                "source_name": source_name,
                "source_url": source_url,
                "source_hash": source_hash,
                "recipe_name": recipe_name,
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
