import polars as pl

from src.quality import diff_frames, merge_frames, quarantine_required


def test_quarantine_required_separates_bad_rows():
    df = pl.DataFrame({
        "name": ["Alpha", None, "Gamma"],
        "city": ["Fresno", "Fresno", ""],
    })
    good, bad = quarantine_required(df, ["name", "city"])
    assert good.height == 1
    assert bad.height == 2
    assert "__error_reason" in bad.columns


def test_diff_frames_marks_added_removed_changed():
    old = pl.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    new = pl.DataFrame({"id": [1, 2, 4], "name": ["A", "Bee", "D"]})
    result = diff_frames(old, new, ["id"])
    statuses = dict(zip(result["id"].to_list(), result["__change_status"].to_list()))
    assert statuses[1] == "Unchanged"
    assert statuses[2] == "Changed"
    assert statuses[3] == "Removed"
    assert statuses[4] == "Added"


def test_merge_frames_left_join():
    left = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    right = pl.DataFrame({"id": [2], "phone": ["555"]})
    result = merge_frames(left, right, ["id"], "left")
    assert result.height == 2
    assert "phone" in result.columns
