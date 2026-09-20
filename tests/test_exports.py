import io
import json
import zipfile

import polars as pl

from src.exports import to_validation_zip


def test_validation_zip_contains_clean_quarantine_profile_and_manifest():
    clean = pl.DataFrame({"name": ["Alpha"], "city": ["Fresno"]})
    quarantine = pl.DataFrame({
        "name": [None],
        "city": ["Fresno"],
        "__error_reason": ["Missing one or more required fields"],
    })

    payload = to_validation_zip(
        clean,
        "providers",
        profile=[{"column": "name", "dtype": "String"}],
        quarantine=quarantine,
        transform_config={"trim_strings": True},
    )

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = set(archive.namelist())
        assert "providers_clean.csv" in names
        assert "providers_quarantine.csv" in names
        assert "providers_profile.json" in names
        assert "providers_transform_recipe.json" in names
        assert "providers_manifest.json" in names

        manifest = json.loads(archive.read("providers_manifest.json"))
        assert manifest["rows_clean"] == 1
        assert manifest["rows_quarantined"] == 1
