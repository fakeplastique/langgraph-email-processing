import pytest

from email_processor.blob_store import LocalFileBlobStore


class TestLocalFileBlobStore:
    def test_write_and_read(self, tmp_path):
        store = LocalFileBlobStore(str(tmp_path))
        store.write("hello.txt", "Hello World")
        assert store.read("hello.txt") == "Hello World"

    def test_read_missing_file_raises(self, tmp_path):
        store = LocalFileBlobStore(str(tmp_path))
        with pytest.raises(FileNotFoundError):
            store.read("nonexistent.txt")

    def test_nested_directory_creation(self, tmp_path):
        store = LocalFileBlobStore(str(tmp_path))
        store.write("a/b/c/deep.txt", "nested content")
        assert store.read("a/b/c/deep.txt") == "nested content"

    def test_overwrite_existing(self, tmp_path):
        store = LocalFileBlobStore(str(tmp_path))
        store.write("file.txt", "v1")
        store.write("file.txt", "v2")
        assert store.read("file.txt") == "v2"
