from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import polars as pl
import requests


CENSUS_ENDPOINT = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"


def census_geocode_one(address: str, timeout: int = 30) -> dict[str, Any]:
    clean = (address or "").strip()
    if not clean:
        return {
            "input": address,
            "status": "blank",
            "matched_address": None,
            "latitude": None,
            "longitude": None,
        }

    response = requests.get(
        CENSUS_ENDPOINT,
        params={
            "address": clean,
            "benchmark": "Public_AR_Current",
            "format": "json",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    matches = response.json().get("result", {}).get("addressMatches", [])

    if not matches:
        return {
            "input": clean,
            "status": "no_match",
            "matched_address": None,
            "latitude": None,
            "longitude": None,
        }

    match = matches[0]
    coords = match.get("coordinates") or {}
    return {
        "input": clean,
        "status": "matched",
        "matched_address": match.get("matchedAddress"),
        "latitude": coords.get("y"),
        "longitude": coords.get("x"),
    }


def geocode_dataframe(
    df: pl.DataFrame,
    address_column: str,
    max_rows: int = 500,
    workers: int = 4,
) -> pl.DataFrame:
    if address_column not in df.columns:
        raise ValueError(f"Address column not found: {address_column}")
    if max_rows < 1:
        raise ValueError("max_rows must be at least 1")

    addresses = (
        df.get_column(address_column)
        .cast(pl.String, strict=False)
        .fill_null("")
        .to_list()
    )

    unique = []
    seen = set()
    for address in addresses:
        clean = address.strip()
        if clean and clean not in seen:
            seen.add(clean)
            unique.append(clean)
        if len(unique) >= max_rows:
            break

    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as pool:
        futures = {pool.submit(census_geocode_one, address): address for address in unique}
        for future in as_completed(futures):
            address = futures[future]
            try:
                results[address] = future.result()
            except Exception as exc:
                results[address] = {
                    "input": address,
                    "status": "error",
                    "matched_address": None,
                    "latitude": None,
                    "longitude": None,
                    "error": str(exc),
                }

    statuses = []
    matched = []
    lats = []
    longs = []
    attempted = set(results)

    for address in addresses:
        clean = address.strip()
        if not clean:
            statuses.append("blank")
            matched.append(None)
            lats.append(None)
            longs.append(None)
            continue
        if clean not in attempted:
            statuses.append("not_attempted_limit")
            matched.append(None)
            lats.append(None)
            longs.append(None)
            continue
        result = results[clean]
        statuses.append(result.get("status"))
        matched.append(result.get("matched_address"))
        lats.append(result.get("latitude"))
        longs.append(result.get("longitude"))

    return df.with_columns([
        pl.Series("__geocode_status", statuses, dtype=pl.String),
        pl.Series("__matched_address", matched, dtype=pl.String),
        pl.Series("latitude", lats, dtype=pl.Float64),
        pl.Series("longitude", longs, dtype=pl.Float64),
    ])
