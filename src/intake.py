from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Dict

import pandas as pd
import polars as pl
import requests


SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt", ".parquet", ".json", ".jsonl", ".ndjson", ".xlsx", ".xls", ".zip"}


class IntakeError(ValueError):
    pass


def _safe_name(name: str) -> str:
    base = Path(name).stem.strip() or "dataset"
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in base)


def _read_csv_bytes(data: bytes, separator: str | None = None) -> pl.DataFrame:
    source = io.BytesIO(data)
    kwargs = {
        "infer_schema_length": 10_000,
        "ignore_errors": True,
        "try_parse_dates": True,
        "null_values": ["", "NULL", "null", "N/A", "n/a", "NA"],
    }
    if separator:
        kwargs["separator"] = separator
    try:
        return pl.read_csv(source, **kwargs)
    except Exception:
        source.seek(0)
        pdf = pd.read_csv(source, sep=separator or None, engine="python", dtype_backend="pyarrow")
        return pl.from_pandas(pdf)


def _read_json_bytes(data: bytes, ndjson: bool = False) -> pl.DataFrame:
    source = io.BytesIO(data)
    if ndjson:
        return pl.read_ndjson(source)
    try:
        return pl.read_json(source)
    except Exception:
        payload = json.loads(data.decode("utf-8-sig"))
        if isinstance(payload, dict):
            for value in payload.values():
                if isinstance(value, list):
                    payload = value
                    break
        return pl.DataFrame(payload)


def _read_excel_bytes(data: bytes) -> Dict[str, pl.DataFrame]:
    book = pd.read_excel(io.BytesIO(data), sheet_name=None, dtype_backend="pyarrow")
    return {str(sheet): pl.from_pandas(frame) for sheet, frame in book.items()}


def load_bytes(filename: str, data: bytes) -> Dict[str, pl.DataFrame]:
    ext = Path(filename).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise IntakeError(f"Unsupported file type: {ext or 'unknown'}")

    stem = _safe_name(filename)

    if ext == ".zip":
        result: Dict[str, pl.DataFrame] = {}
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for member in archive.infolist():
                if member.is_dir() or member.filename.startswith("__MACOSX/"):
                    continue
                member_ext = Path(member.filename).suffix.lower()
                if member_ext not in SUPPORTED_EXTENSIONS - {".zip"}:
                    continue
                inner = archive.read(member)
                for key, frame in load_bytes(member.filename, inner).items():
                    candidate = f"{stem}__{key}"
                    suffix = 2
                    while candidate in result:
                        candidate = f"{stem}__{key}_{suffix}"
                        suffix += 1
                    result[candidate] = frame
        if not result:
            raise IntakeError("The ZIP did not contain a supported tabular file.")
        return result

    if ext in {".csv", ".txt"}:
        return {stem: _read_csv_bytes(data)}
    if ext == ".tsv":
        return {stem: _read_csv_bytes(data, "\t")}
    if ext == ".parquet":
        return {stem: pl.read_parquet(io.BytesIO(data))}
    if ext in {".jsonl", ".ndjson"}:
        return {stem: _read_json_bytes(data, ndjson=True)}
    if ext == ".json":
        return {stem: _read_json_bytes(data)}
    if ext in {".xlsx", ".xls"}:
        sheets = _read_excel_bytes(data)
        if len(sheets) == 1:
            return {stem: next(iter(sheets.values()))}
        return {f"{stem}__{_safe_name(sheet)}": frame for sheet, frame in sheets.items()}

    raise IntakeError(f"Could not load {filename}")


def load_url(url: str, timeout: int = 60) -> Dict[str, pl.DataFrame]:
    response = requests.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    filename = Path(response.url.split("?", 1)[0]).name or "download.csv"

    if Path(filename).suffix.lower() not in SUPPORTED_EXTENSIONS:
        content_type = response.headers.get("content-type", "").lower()
        if "csv" in content_type or "text/plain" in content_type:
            filename = "download.csv"
        elif "json" in content_type:
            filename = "download.json"
        elif "parquet" in content_type:
            filename = "download.parquet"
        else:
            raise IntakeError(
                "The URL downloaded successfully, but the file type could not be identified. "
                "Use a direct CSV, XLSX, ZIP, JSON, or Parquet URL."
            )
    return load_bytes(filename, response.content)
