from pathlib import Path

from streamlit.testing.v1 import AppTest


def test_app_starts_without_exception():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    app = AppTest.from_file(app_path)
    app.run(timeout=30)
    assert not app.exception
