from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import requests

from src.database import (
    get_source,
    list_recipes,
    update_source_refresh_state,
    write_frame,
)
from src.intake import load_bytes
from src.transform import apply_transform


@dataclass
class DownloadedSource:
    url: str
    filename: str
    content: bytes
    sha256: str
    etag: str | None
    last_modified: str | None
    content_type: str | None


def download_source(url: str, timeout: int = 90) -> DownloadedSource:
    response = requests.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()

    filename = Path(response.url.split("?", 1)[0]).name or "download.csv"
    content_type = response.headers.get("content-type")
    suffix = Path(filename).suffix.lower()

    if not suffix:
        lower_type = (content_type or "").lower()
        if "csv" in lower_type or "text/plain" in lower_type:
            filename = "download.csv"
        elif "json" in lower_type:
            filename = "download.json"
        elif "parquet" in lower_type:
            filename = "download.parquet"
        elif "zip" in lower_type:
            filename = "download.zip"
        elif "spreadsheet" in lower_type or "excel" in lower_type:
            filename = "download.xlsx"

    content = response.content
    return DownloadedSource(
        url=response.url,
        filename=filename,
        content=content,
        sha256=hashlib.sha256(content).hexdigest(),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
        content_type=content_type,
    )


def _recipe_config(recipe_name: str | None) -> dict[str, Any] | None:
    if not recipe_name:
        return None
    for recipe in list_recipes():
        if recipe["name"] == recipe_name:
            return recipe["config"]
    raise ValueError(f"Saved recipe not found: {recipe_name}")


def refresh_source(source_id: int, force: bool = False) -> dict[str, Any]:
    source = get_source(source_id)
    if not source:
        raise ValueError("Source not found.")

    downloaded = download_source(source["url"])
    previous_hash = source.get("last_hash")

    if previous_hash == downloaded.sha256 and not force:
        update_source_refresh_state(
            source_id,
            status="unchanged",
            source_hash=downloaded.sha256,
            etag=downloaded.etag,
            last_modified=downloaded.last_modified,
            changed=False,
            error=None,
        )
        return {
            "source_id": source_id,
            "status": "unchanged",
            "hash": downloaded.sha256,
            "tables": [],
            "rows": 0,
        }

    frames = load_bytes(downloaded.filename, downloaded.content)
    recipe = _recipe_config(source.get("recipe_name"))
    written = []
    total_rows = 0

    for index, (dataset_name, frame) in enumerate(frames.items()):
        working = apply_transform(frame, recipe) if recipe else frame
        target_base = source["target_table"]
        target = target_base if len(frames) == 1 else f"{target_base}_{dataset_name}"
        result = write_frame(
            working,
            target,
            source_name=source["name"],
            mode="replace",
        )
        written.append({
            "dataset": dataset_name,
            "table": result["table"],
            "rows": result["rows"],
            "snapshot_table": result["snapshot_table"],
        })
        total_rows += result["rows"]

    update_source_refresh_state(
        source_id,
        status="updated",
        source_hash=downloaded.sha256,
        etag=downloaded.etag,
        last_modified=downloaded.last_modified,
        changed=True,
        error=None,
    )

    return {
        "source_id": source_id,
        "status": "updated",
        "hash": downloaded.sha256,
        "tables": written,
        "rows": total_rows,
    }


def refresh_all_active_sources(force: bool = False) -> list[dict[str, Any]]:
    from src.database import list_sources

    results = []
    for source in list_sources(active_only=True):
        try:
            results.append(refresh_source(source["id"], force=force))
        except Exception as exc:
            update_source_refresh_state(
                source["id"],
                status="error",
                changed=False,
                error=str(exc),
            )
            results.append({
                "source_id": source["id"],
                "status": "error",
                "error": str(exc),
            })
    return results
