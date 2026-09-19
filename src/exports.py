from __future__ import annotations

import io

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
