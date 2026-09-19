from __future__ import annotations

import re
from typing import Any, Dict

import polars as pl


def clean_column_name(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "column"


def standardize_columns(df: pl.DataFrame) -> pl.DataFrame:
    used: dict[str, int] = {}
    mapping: dict[str, str] = {}
    for old in df.columns:
        base = clean_column_name(old)
        used[base] = used.get(base, 0) + 1
        mapping[old] = base if used[base] == 1 else f"{base}_{used[base]}"
    return df.rename(mapping)


def trim_string_columns(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    for col, dtype in df.schema.items():
        if dtype == pl.String:
            exprs.append(
                pl.when(pl.col(col).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(col).str.strip_chars())
                .alias(col)
            )
        else:
            exprs.append(pl.col(col))
    return df.select(exprs)


def remove_blank_rows(df: pl.DataFrame) -> pl.DataFrame:
    if not df.columns:
        return df
    checks = []
    for col, dtype in df.schema.items():
        if dtype == pl.String:
            checks.append(pl.col(col).is_not_null() & (pl.col(col).str.strip_chars() != ""))
        else:
            checks.append(pl.col(col).is_not_null())
    predicate = checks[0]
    for check in checks[1:]:
        predicate = predicate | check
    return df.filter(predicate)


def normalize_phone_expr(column: str) -> pl.Expr:
    digits = pl.col(column).cast(pl.String, strict=False).str.replace_all(r"\D+", "")
    return (
        pl.when(digits.str.len_chars() == 10)
        .then(pl.concat_str([
            pl.lit("("), digits.str.slice(0, 3), pl.lit(") "),
            digits.str.slice(3, 3), pl.lit("-"), digits.str.slice(6, 4),
        ]))
        .when((digits.str.len_chars() == 11) & digits.str.starts_with("1"))
        .then(pl.concat_str([
            pl.lit("("), digits.str.slice(1, 3), pl.lit(") "),
            digits.str.slice(4, 3), pl.lit("-"), digits.str.slice(7, 4),
        ]))
        .otherwise(pl.col(column).cast(pl.String, strict=False).str.strip_chars())
        .alias(column)
    )


def provider_type_expr(source_column: str, output_column: str = "provider_type") -> pl.Expr:
    text = pl.col(source_column).cast(pl.String, strict=False).str.to_lowercase().fill_null("")
    return (
        pl.when(text.str.contains(r"hospital|medical center|health system"))
        .then(pl.lit("Hospital / Medical Center"))
        .when(text.str.contains(r"dental|dentist|orthodont"))
        .then(pl.lit("Dental"))
        .when(text.str.contains(r"pharmacy|drug store|drugstore"))
        .then(pl.lit("Pharmacy"))
        .when(text.str.contains(r"laborator|labcorp|quest diagnostics|clinical lab"))
        .then(pl.lit("Laboratory"))
        .when(text.str.contains(r"cardiolog|imaging|radiolog|diagnostic"))
        .then(pl.lit("Diagnostic / Specialty"))
        .when(text.str.contains(r"occupational|work health|employee health"))
        .then(pl.lit("Occupational Health"))
        .when(text.str.contains(r"clinic|urgent care|family medicine|primary care|medical group"))
        .then(pl.lit("Clinic / Physician Practice"))
        .otherwise(pl.lit("Other / Review"))
        .alias(output_column)
    )


def apply_transform(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    out = df.clone()

    if config.get("standardize_columns"):
        out = standardize_columns(out)

    if config.get("trim_strings"):
        out = trim_string_columns(out)

    rename_map = {
        old: new.strip()
        for old, new in (config.get("rename_map") or {}).items()
        if old in out.columns and isinstance(new, str) and new.strip()
    }
    if rename_map:
        out = out.rename(rename_map)

    drop_columns = [c for c in (config.get("drop_columns") or []) if c in out.columns]
    if drop_columns:
        out = out.drop(drop_columns)

    for column in config.get("titlecase_columns") or []:
        if column in out.columns:
            out = out.with_columns(
                pl.col(column).cast(pl.String, strict=False).str.to_titlecase().alias(column)
            )

    phone_column = config.get("phone_column")
    if phone_column and phone_column in out.columns:
        out = out.with_columns(normalize_phone_expr(phone_column))

    provider_source = config.get("provider_type_source")
    if provider_source and provider_source in out.columns:
        out = out.with_columns(provider_type_expr(provider_source))

    dedupe_columns = [c for c in (config.get("dedupe_columns") or []) if c in out.columns]
    if dedupe_columns:
        out = out.unique(subset=dedupe_columns, keep="first", maintain_order=True)

    if config.get("remove_blank_rows"):
        out = remove_blank_rows(out)

    return out


def profile_frame(df: pl.DataFrame) -> list[dict[str, Any]]:
    rows = max(df.height, 1)
    result: list[dict[str, Any]] = []
    for col, dtype in df.schema.items():
        series = df.get_column(col)
        nulls = series.null_count()
        unique = series.n_unique()
        result.append({
            "column": col,
            "dtype": str(dtype),
            "nulls": nulls,
            "null_pct": round((nulls / rows) * 100, 2),
            "unique": unique,
        })
    return result
