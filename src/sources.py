from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from src.archive import archive_bytes, uploadcare_available
from src.database import (
    get_source,
    list_due_sources,
    list_recipes,
    list_sources,
    update_source_refresh_state,
    write_frame,
)
from src.intake import load_bytes
from src.transform import apply_transform


@dataclass
class DownloadedSource:
    url: str
    filename: str
    content: bytes | None
    sha256: str | None
    etag: str | None
    last_modified: str | None
    content_type: str | None
    not_modified: bool = False


def download_source(
    url: str,
    timeout: int = 90,
    etag: str | None = None,
    last_modified: str | None = None,
) -> DownloadedSource:
    headers: dict[str, str] = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    response = requests.get(
        url,
        timeout=timeout,
        allow_redirects=True,
        headers=headers,
    )

    filename = Path(response.url.split("?", 1)[0]).name or "download.csv"
    content_type = response.headers.get("content-type")

    if response.status_code == 304:
        return DownloadedSource(
            url=response.url,
            filename=filename,
            content=None,
            sha256=None,
            etag=response.headers.get("etag") or etag,
            last_modified=response.headers.get("last-modified") or last_modified,
            content_type=content_type,
            not_modified=True,
        )

    response.raise_for_status()

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

    body = response.content
    return DownloadedSource(
        url=response.url,
        filename=filename,
        content=body,
        sha256=hashlib.sha256(body).hexdigest(),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
        content_type=content_type,
        not_modified=False,
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

    downloaded = download_source(
        source["url"],
        etag=None if force else source.get("last_etag"),
        last_modified=None if force else source.get("last_modified"),
    )

    if downloaded.not_modified and not force:
        update_source_refresh_state(
            source_id,
            status="unchanged",
            etag=downloaded.etag,
            last_modified=downloaded.last_modified,
            changed=False,
            error=None,
        )
        return {
            "source_id": source_id,
            "source_name": source["name"],
            "status": "unchanged",
            "reason": "HTTP 304",
            "hash": source.get("last_hash"),
            "tables": [],
            "rows": 0,
        }

    if downloaded.content is None or downloaded.sha256 is None:
        raise RuntimeError("Source response did not contain downloadable content.")

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
            "source_name": source["name"],
            "status": "unchanged",
            "reason": "SHA-256 unchanged",
            "hash": downloaded.sha256,
            "tables": [],
            "rows": 0,
        }

    archive = None
    archive_warning = None
    if source.get("archive_raw"):
        if uploadcare_available():
            try:
                archive = archive_bytes(
                    downloaded.filename,
                    downloaded.content,
                    source_name=source["name"],
                    source_url=source["url"],
                    source_hash=downloaded.sha256,
                )
            except Exception as exc:
                archive_warning = f"Raw archive failed: {exc}"
        else:
            archive_warning = "Raw archive requested but UPLOADCARE_PUBLIC_KEY is not configured."

    frames = load_bytes(downloaded.filename, downloaded.content)
    recipe = _recipe_config(source.get("recipe_name"))
    written = []
    total_rows = 0

    for dataset_name, frame in frames.items():
        working = apply_transform(frame, recipe) if recipe else frame
        target_base = source["target_table"]
        target = target_base if len(frames) == 1 else f"{target_base}_{dataset_name}"
        result = write_frame(
            working,
            target,
            source_name=source["name"],
            mode="replace",
            source_id=source["id"],
            source_url=source["url"],
            source_hash=downloaded.sha256,
            recipe_name=source.get("recipe_name"),
            archive_uuid=archive.get("uuid") if archive else None,
            archive_url=archive.get("url") if archive else None,
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
        archive_uuid=archive.get("uuid") if archive else None,
        archive_url=archive.get("url") if archive else None,
    )

    return {
        "source_id": source_id,
        "source_name": source["name"],
        "status": "updated",
        "hash": downloaded.sha256,
        "archive": archive,
        "archive_warning": archive_warning,
        "tables": written,
        "rows": total_rows,
    }


def refresh_all_active_sources(
    force: bool = False,
    due_only: bool = False,
) -> list[dict[str, Any]]:
    sources = list_due_sources() if due_only else list_sources(active_only=True)

    results = []
    for source in sources:
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
                "source_name": source["name"],
                "status": "error",
                "error": str(exc),
            })
    return results
