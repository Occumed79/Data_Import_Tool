from __future__ import annotations

import io
import json
import zipfile
from typing import Any

import polars as pl


def to_csv_bytes(df: pl.DataFrame) -> bytes:
    return df.write_csv().encode("utf-8")


def to_json_bytes(df: pl.DataFrame) -> bytes:
    return df.write_json().encode("utf-8")


def to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def to_excel_bytes(df: pl.DataFrame, sheet_name: str = "Data") -> bytes:
    buffer = io.BytesIO()
    with __import__("pandas").ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_pandas().to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buffer.getvalue()


def to_validation_zip(
    df: pl.DataFrame,
    dataset_name: str,
    profile: list[dict[str, Any]] | None = None,
    quarantine: pl.DataFrame | None = None,
    transform_config: dict[str, Any] | None = None,
) -> bytes:
    buffer = io.BytesIO()
    safe_name = "".join(
        ch if ch.isalnum() or ch in "-_" else "_"
        for ch in dataset_name
    ).strip("_") or "dataset"

    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{safe_name}_clean.csv", to_csv_bytes(df))

        if quarantine is not None and quarantine.height:
            archive.writestr(
                f"{safe_name}_quarantine.csv",
                to_csv_bytes(quarantine),
            )

        if profile is not None:
            archive.writestr(
                f"{safe_name}_profile.json",
                json.dumps(profile, indent=2, default=str),
            )

        if transform_config:
            archive.writestr(
                f"{safe_name}_transform_recipe.json",
                json.dumps(transform_config, indent=2, default=str),
            )

        manifest = {
            "dataset": dataset_name,
            "rows_clean": df.height,
            "columns_clean": df.width,
            "rows_quarantined": quarantine.height if quarantine is not None else 0,
            "files": archive.namelist(),
        }
        archive.writestr(
            f"{safe_name}_manifest.json",
            json.dumps(manifest, indent=2, default=str),
        )

    return buffer.getvalue()
