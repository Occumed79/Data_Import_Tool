import polars as pl

from src.normalize import add_address_key, find_fuzzy_duplicates, normalize_postal, normalize_street


def test_address_normalization():
    assert normalize_street("123 Main Street.") == "123 MAIN ST"
    assert normalize_postal("93720-1234") == "93720"


def test_add_address_key():
    df = pl.DataFrame({
        "street": ["123 Main Street"],
        "city": ["Fresno"],
        "state": ["ca"],
        "zip": ["93720-1234"],
    })
    out = add_address_key(df, "street", "city", "state", "zip")
    assert out["__address_key"][0] == "123 MAIN ST | FRESNO | CA | 93720"


def test_fuzzy_duplicates_with_block():
    df = pl.DataFrame({
        "name": ["Saint Agnes Medical Center", "St Agnes Medical Ctr", "Different Clinic"],
        "city": ["Fresno", "Fresno", "Fresno"],
    })
    pairs = find_fuzzy_duplicates(df, "name", ["city"], threshold=75)
    assert pairs.height >= 1
    assert pairs["score"][0] >= 75
