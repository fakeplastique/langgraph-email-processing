from abc import ABC, abstractmethod
from pathlib import Path


class BlobStore(ABC):
    @abstractmethod
    def read(self, path: str) -> str:
        ...

    @abstractmethod
    def write(self, path: str, content: str) -> None:
        ...


class LocalFileBlobStore(BlobStore):
    def __init__(self, root_dir: str):
        self._root = Path(root_dir)

    def read(self, path: str) -> str:
        return (self._root / path).read_text(encoding="utf-8")

    def write(self, path: str, content: str) -> None:
        full = self._root / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")
