from __future__ import annotations

import os
from typing import Any

import requests


UPLOADCARE_UPLOAD_URL = "https://upload.uploadcare.com/base/"


def uploadcare_public_key() -> str | None:
    return os.getenv("UPLOADCARE_PUBLIC_KEY") or os.getenv("UPLOADCARE_PUB_KEY")


def uploadcare_available() -> bool:
    return bool(uploadcare_public_key())


def archive_bytes(
    filename: str,
    content: bytes,
    source_name: str | None = None,
    source_url: str | None = None,
    source_hash: str | None = None,
    timeout: int = 120,
) -> dict[str, Any]:
    public_key = uploadcare_public_key()
    if not public_key:
        raise RuntimeError("UPLOADCARE_PUBLIC_KEY is not configured.")

    data: dict[str, str] = {
        "UPLOADCARE_PUB_KEY": public_key,
        "UPLOADCARE_STORE": "1",
        "tags": "occu-med,data-import-tool,raw-source",
    }
    if source_name:
        data["metadata[source_name]"] = source_name[:500]
    if source_url:
        data["metadata[source_url]"] = source_url[:500]
    if source_hash:
        data["metadata[source_hash]"] = source_hash[:500]

    response = requests.post(
        UPLOADCARE_UPLOAD_URL,
        data=data,
        files={"file": (filename, content, "application/octet-stream")},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    uuid = payload.get("file")
    if not uuid:
        raise RuntimeError(f"Uploadcare did not return a file UUID: {payload}")

    return {
        "uuid": uuid,
        "url": f"https://ucarecdn.com/{uuid}/",
        "filename": filename,
        "size": len(content),
    }
