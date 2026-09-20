from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import polars as pl
import requests


CENSUS_ENDPOINT = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
MAPBOX_BATCH_ENDPOINT = "https://api.mapbox.com/search/geocode/v6/batch"


def mapbox_access_token() -> str | None:
    return os.getenv("MAPBOX_ACCESS_TOKEN") or os.getenv("MAPBOX_TOKEN")


def mapbox_available() -> bool:
    return bool(mapbox_access_token())


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

    return _attach_geocode_results(df, address_column, results, provider="census")


def _parse_mapbox_result(address: str, result: dict[str, Any]) -> dict[str, Any]:
    features = result.get("features") or []
    if not features:
        return {
            "input": address,
            "status": "no_match",
            "matched_address": None,
            "latitude": None,
            "longitude": None,
            "confidence": None,
        }

    feature = features[0]
    geometry = feature.get("geometry") or {}
    coordinates = geometry.get("coordinates") or [None, None]
    properties = feature.get("properties") or {}
    match_code = properties.get("match_code") or {}

    matched = (
        properties.get("full_address")
        or properties.get("name_preferred")
        or properties.get("name")
    )
    place_formatted = properties.get("place_formatted")
    if matched and place_formatted and place_formatted not in matched:
        matched = f"{matched}, {place_formatted}"
    if not matched:
        matched = feature.get("place_name")

    return {
        "input": address,
        "status": "matched",
        "matched_address": matched,
        "latitude": coordinates[1] if len(coordinates) > 1 else None,
        "longitude": coordinates[0] if coordinates else None,
        "confidence": match_code.get("confidence"),
    }


def mapbox_geocode_dataframe(
    df: pl.DataFrame,
    address_column: str,
    max_rows: int = 1000,
    permanent: bool = False,
    timeout: int = 90,
) -> pl.DataFrame:
    token = mapbox_access_token()
    if not token:
        raise RuntimeError("MAPBOX_ACCESS_TOKEN is not configured.")
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

    unique: list[str] = []
    seen: set[str] = set()
    for address in addresses:
        clean = address.strip()
        if clean and clean not in seen:
            seen.add(clean)
            unique.append(clean)
        if len(unique) >= max_rows:
            break

    results: dict[str, dict[str, Any]] = {}

    for start in range(0, len(unique), 1000):
        batch_addresses = unique[start : start + 1000]
        payload = [
            {
                "q": address,
                "limit": 1,
                "autocomplete": False,
            }
            for address in batch_addresses
        ]

        response = requests.post(
            MAPBOX_BATCH_ENDPOINT,
            params={
                "access_token": token,
                "permanent": str(bool(permanent)).lower(),
            },
            json=payload,
            timeout=timeout,
        )
        response.raise_for_status()
        batch_results = response.json().get("batch") or []

        for index, address in enumerate(batch_addresses):
            if index < len(batch_results):
                results[address] = _parse_mapbox_result(address, batch_results[index])
            else:
                results[address] = {
                    "input": address,
                    "status": "error",
                    "matched_address": None,
                    "latitude": None,
                    "longitude": None,
                    "confidence": None,
                    "error": "Mapbox batch response was shorter than the request.",
                }

    return _attach_geocode_results(df, address_column, results, provider="mapbox")


def _attach_geocode_results(
    df: pl.DataFrame,
    address_column: str,
    results: dict[str, dict[str, Any]],
    provider: str,
) -> pl.DataFrame:
    addresses = (
        df.get_column(address_column)
        .cast(pl.String, strict=False)
        .fill_null("")
        .to_list()
    )
    statuses = []
    matched = []
    lats = []
    longs = []
    confidences = []
    attempted = set(results)

    for address in addresses:
        clean = address.strip()
        if not clean:
            statuses.append("blank")
            matched.append(None)
            lats.append(None)
            longs.append(None)
            confidences.append(None)
            continue
        if clean not in attempted:
            statuses.append("not_attempted_limit")
            matched.append(None)
            lats.append(None)
            longs.append(None)
            confidences.append(None)
            continue

        result = results[clean]
        statuses.append(result.get("status"))
        matched.append(result.get("matched_address"))
        lats.append(result.get("latitude"))
        longs.append(result.get("longitude"))
        confidences.append(result.get("confidence"))

    return df.with_columns([
        pl.Series("__geocode_provider", [provider] * df.height, dtype=pl.String),
        pl.Series("__geocode_status", statuses, dtype=pl.String),
        pl.Series("__matched_address", matched, dtype=pl.String),
        pl.Series("__geocode_confidence", confidences, dtype=pl.String),
        pl.Series("latitude", lats, dtype=pl.Float64),
        pl.Series("longitude", longs, dtype=pl.Float64),
    ])
