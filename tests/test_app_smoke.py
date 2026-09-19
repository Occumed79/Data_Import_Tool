from streamlit.testing.v1 import AppTest


def test_app_starts_without_exception():
    app = AppTest.from_file("app.py")
    app.run(timeout=30)
    assert not app.exception
