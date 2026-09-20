import hashlib

from src.sources import DownloadedSource


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
