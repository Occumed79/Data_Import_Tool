import polars as pl

from src.geocode import _parse_mapbox_result, _attach_geocode_results


def test_parse_mapbox_result():
    result = {
        "features": [{
            "geometry": {"coordinates": [-119.7871, 36.7378]},
            "properties": {
                "full_address": "123 Main Street, Fresno, California 93721, United States",
                "match_code": {"confidence": "high"},
            },
        }]
    }
    parsed = _parse_mapbox_result("123 Main St Fresno CA", result)
    assert parsed["status"] == "matched"
    assert parsed["longitude"] == -119.7871
    assert parsed["latitude"] == 36.7378
    assert parsed["confidence"] == "high"


def test_attach_geocode_results_marks_unattempted_rows():
    df = pl.DataFrame({"address": ["A", "B", ""]})
    results = {
        "A": {
            "status": "matched",
            "matched_address": "A MATCH",
            "latitude": 1.0,
            "longitude": 2.0,
            "confidence": "high",
        }
    }
    out = _attach_geocode_results(df, "address", results, provider="mapbox")
    assert out["__geocode_status"].to_list() == ["matched", "not_attempted_limit", "blank"]
    assert out["__geocode_provider"].to_list() == ["mapbox", "mapbox", "mapbox"]
