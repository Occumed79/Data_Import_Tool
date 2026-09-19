import polars as pl

from src.transform import apply_transform, standardize_columns


def test_standardize_columns_dedupes_names():
    df = pl.DataFrame({"Clinic Name": [" A "], "Clinic-Name": ["B"]})
    out = standardize_columns(df)
    assert out.columns == ["clinic_name", "clinic_name_2"]


def test_transform_resolves_original_names_after_standardization():
    df = pl.DataFrame({
        "Name": [" Alpha Clinic ", " Alpha Clinic "],
        "Phone": ["5594352800", "5594352800"],
    })
    out = apply_transform(df, {
        "standardize_columns": True,
        "trim_strings": True,
        "phone_column": "Phone",
        "dedupe_columns": ["Name", "Phone"],
        "remove_blank_rows": True,
    })
    assert out.height == 1
    assert out["name"][0] == "Alpha Clinic"
    assert out["phone"][0] == "(559) 435-2800"
