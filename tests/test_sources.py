import hashlib

from src.sources import DownloadedSource, download_source


class FakeResponse:
    def __init__(self, status_code, url, content=b"", headers=None, json_data=None):
        self.status_code = status_code
        self.url = url
        self.content = content
        self.headers = headers or {}
        self._json_data = json_data or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data


def test_downloaded_source_hash_shape():
    payload = b"hello-world"
    source = DownloadedSource(
        url="https://example.test/data.csv",
        filename="data.csv",
        content=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        etag=None,
        last_modified=None,
        content_type="text/csv",
    )
    assert len(source.sha256) == 64
    assert source.filename == "data.csv"


def test_download_source_uses_conditional_headers(monkeypatch):
    captured = {}

    def fake_get(url, timeout, allow_redirects, headers):
        captured["headers"] = headers
        return FakeResponse(
            304,
            "https://example.test/data.csv",
            headers={
                "etag": '"abc"',
                "last-modified": "Sat, 20 Sep 2026 00:00:00 GMT",
            },
        )

    monkeypatch.setattr("src.sources.requests.get", fake_get)
    result = download_source(
        "https://example.test/data.csv",
        etag='"abc"',
        last_modified="Sat, 20 Sep 2026 00:00:00 GMT",
    )

    assert captured["headers"]["If-None-Match"] == '"abc"'
    assert "If-Modified-Since" in captured["headers"]
    assert result.not_modified is True
    assert result.content is None


def test_download_source_hashes_changed_content(monkeypatch):
    payload = b"a,b\n1,2\n"

    def fake_get(url, timeout, allow_redirects, headers):
        return FakeResponse(
            200,
            "https://example.test/data.csv",
            content=payload,
            headers={"content-type": "text/csv", "etag": '"new"'},
        )

    monkeypatch.setattr("src.sources.requests.get", fake_get)
    result = download_source("https://example.test/data.csv")

    assert result.not_modified is False
    assert result.sha256 == hashlib.sha256(payload).hexdigest()
    assert result.filename == "data.csv"
