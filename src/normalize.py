from __future__ import annotations

import re
from itertools import combinations
from typing import Iterable

import polars as pl
from rapidfuzz.fuzz import token_set_ratio


_SUFFIXES = {
    "STREET": "ST",
    "ST": "ST",
    "ROAD": "RD",
    "RD": "RD",
    "AVENUE": "AVE",
    "AVE": "AVE",
    "BOULEVARD": "BLVD",
    "BLVD": "BLVD",
    "DRIVE": "DR",
    "DR": "DR",
    "LANE": "LN",
    "LN": "LN",
    "COURT": "CT",
    "CT": "CT",
    "HIGHWAY": "HWY",
    "HWY": "HWY",
    "PARKWAY": "PKWY",
    "PKWY": "PKWY",
    "PLACE": "PL",
    "PL": "PL",
    "TERRACE": "TER",
    "TER": "TER",
    "TRAIL": "TRL",
    "TRL": "TRL",
    "CIRCLE": "CIR",
    "CIR": "CIR",
}


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    text = re.sub(r"[.,#;:/\\]+", " ", text)
    text = re.sub(r"[^A-Z0-9\- ]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_street(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    parts = text.split()
    if parts and parts[-1] in _SUFFIXES:
        parts[-1] = _SUFFIXES[parts[-1]]
    return " ".join(parts)


def normalize_postal(value: object) -> str:
    text = normalize_text(value).replace(" ", "")
    if re.fullmatch(r"\d{5}(?:-?\d{4})?", text):
        return text[:5]
    return text


def add_address_key(
    df: pl.DataFrame,
    street: str,
    city: str,
    state: str,
    postal: str | None = None,
    country: str | None = None,
    output_column: str = "__address_key",
) -> pl.DataFrame:
    required = [street, city, state]
    for column in required:
        if column not in df.columns:
            raise ValueError(f"Address column not found: {column}")

    expressions = [
        pl.col(street).map_elements(normalize_street, return_dtype=pl.String),
        pl.col(city).map_elements(normalize_text, return_dtype=pl.String),
        pl.col(state).map_elements(normalize_text, return_dtype=pl.String),
    ]

    if postal and postal in df.columns:
        expressions.append(pl.col(postal).map_elements(normalize_postal, return_dtype=pl.String))
    if country and country in df.columns:
        expressions.append(pl.col(country).map_elements(normalize_text, return_dtype=pl.String))

    return df.with_columns(
        pl.concat_str(expressions, separator=" | ", ignore_nulls=True).alias(output_column)
    )


def find_fuzzy_duplicates(
    df: pl.DataFrame,
    name_column: str,
    block_columns: Iterable[str] | None = None,
    threshold: float = 92.0,
    max_pairs: int = 50_000,
) -> pl.DataFrame:
    if name_column not in df.columns:
        raise ValueError(f"Name column not found: {name_column}")

    blocks = [c for c in (block_columns or []) if c in df.columns]
    working = df.with_row_index("__row_id").with_columns(
        pl.col(name_column)
        .map_elements(normalize_text, return_dtype=pl.String)
        .alias("__match_name")
    )

    rows = working.to_dicts()
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = tuple(normalize_text(row.get(c)) for c in blocks) if blocks else ("__all__",)
        grouped.setdefault(key, []).append(row)

    results = []
    pair_count = 0

    for block_key, group in grouped.items():
        for left, right in combinations(group, 2):
            pair_count += 1
            if pair_count > max_pairs:
                raise ValueError(
                    f"Candidate comparison limit exceeded ({max_pairs:,}). "
                    "Add a city/state/block column or raise the limit."
                )

            left_name = left.get("__match_name", "")
            right_name = right.get("__match_name", "")
            if not left_name or not right_name:
                continue

            score = float(token_set_ratio(left_name, right_name))
            if score < threshold:
                continue

            results.append({
                "left_row": left["__row_id"],
                "right_row": right["__row_id"],
                "left_name": left.get(name_column),
                "right_name": right.get(name_column),
                "score": round(score, 2),
                "block": " | ".join(block_key),
            })

    if not results:
        return pl.DataFrame({
            "left_row": pl.Series([], dtype=pl.UInt32),
            "right_row": pl.Series([], dtype=pl.UInt32),
            "left_name": pl.Series([], dtype=pl.String),
            "right_name": pl.Series([], dtype=pl.String),
            "score": pl.Series([], dtype=pl.Float64),
            "block": pl.Series([], dtype=pl.String),
        })

    return pl.DataFrame(results).sort("score", descending=True)
