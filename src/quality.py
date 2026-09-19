from __future__ import annotations

from typing import Iterable

import polars as pl


def quarantine_required(
    df: pl.DataFrame,
    required_columns: Iterable[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    required = [c for c in required_columns if c in df.columns]
    if not required:
        return df.clone(), pl.DataFrame(schema=df.schema)

    missing_checks = []
    for column in required:
        dtype = df.schema[column]
        if dtype == pl.String:
            missing = pl.col(column).is_null() | (pl.col(column).str.strip_chars() == "")
        else:
            missing = pl.col(column).is_null()
        missing_checks.append(missing)

    any_missing = pl.any_horizontal(missing_checks)
    good = df.filter(~any_missing)
    bad = df.filter(any_missing).with_columns(
        pl.lit("Missing one or more required fields").alias("__error_reason")
    )
    return good, bad


def merge_frames(
    left: pl.DataFrame,
    right: pl.DataFrame,
    keys: list[str],
    how: str = "left",
) -> pl.DataFrame:
    if not keys:
        raise ValueError("Select at least one join key.")
    for key in keys:
        if key not in left.columns or key not in right.columns:
            raise ValueError(f"Join key not present in both datasets: {key}")
    if how not in {"left", "inner", "full", "semi", "anti"}:
        raise ValueError("Unsupported join mode.")
    return left.join(right, on=keys, how=how, suffix="_right", coalesce=True)


def diff_frames(
    previous: pl.DataFrame,
    current: pl.DataFrame,
    keys: list[str],
) -> pl.DataFrame:
    if not keys:
        raise ValueError("Select at least one comparison key.")
    for key in keys:
        if key not in previous.columns or key not in current.columns:
            raise ValueError(f"Comparison key not present in both datasets: {key}")

    common = [c for c in previous.columns if c in current.columns and c not in keys]
    previous_only = [c for c in previous.columns if c not in current.columns and c not in keys]
    current_only = [c for c in current.columns if c not in previous.columns and c not in keys]

    left = previous.with_columns(pl.lit(True).alias("__previous_present"))
    right_rename = {c: f"{c}__current" for c in common}
    right = (
        current.rename(right_rename)
        .with_columns(pl.lit(True).alias("__current_present"))
    )

    joined = left.join(right, on=keys, how="full", coalesce=True)

    changed_checks = []
    for column in common:
        right_column = f"{column}__current"
        changed_checks.append(
            pl.col(column)
            .cast(pl.String, strict=False)
            .fill_null("__NULL__")
            != pl.col(right_column)
            .cast(pl.String, strict=False)
            .fill_null("__NULL__")
        )

    if previous_only or current_only:
        changed_checks.append(
            pl.lit(bool(previous_only or current_only))
            & pl.col("__previous_present").fill_null(False)
            & pl.col("__current_present").fill_null(False)
        )

    changed_expr = pl.any_horizontal(changed_checks) if changed_checks else pl.lit(False)

    return joined.with_columns(
        pl.when(pl.col("__previous_present").is_null())
        .then(pl.lit("Added"))
        .when(pl.col("__current_present").is_null())
        .then(pl.lit("Removed"))
        .when(changed_expr)
        .then(pl.lit("Changed"))
        .otherwise(pl.lit("Unchanged"))
        .alias("__change_status")
    )
