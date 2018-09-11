"""
Utility for caching general objects.
"""

import shutil
from collections.abc import MutableMapping
from typing import Iterator, Any, Dict

from ._db import *


def fs_dict(dir: str) -> Dict[str, Any]:
    """
    Builder for a filesystem (LevelDB) backed dictionary.
    :param dir: The directory for the dictionary.
    :return: The filesystem-backed dictionary.
    """
    return FileSystemDict(dir)


class FileSystemDict(MutableMapping):
    """
    A str, value dictionary backed by the filesystem.
    """

    def __init__(self, dir):
        super(FileSystemDict, self).__init__()
        self.dir = dir

    def clear(self):
        shutil.rmtree(self.dir)

    def __setitem__(self, k: str, v: Any) -> None:
        from . import _serializer
        with open_db(self.dir) as db:
            db.put(k.encode(), _serializer(v))

    def __delitem__(self, v: str) -> None:
        with open_db(self.dir) as db:
            db.delete(v.encode())

    def __getitem__(self, k: str) -> Any:
        from . import _deserializer
        with open_db(self.dir) as db:
            val = db.get(k.encode())
            if val:
                return _deserializer(val)
            else:
                return None

    def __len__(self) -> int:
        with open_db(self.dir) as db:
            snapshot = db.snapshot()

        count = 0
        for i in snapshot:
            count += 1

        snapshot.close()
        return count

    def __iter__(self) -> Iterator[Any]:
        with open_db(self.dir) as db:
            with db.snapshot() as snapshot:
                yield from snapshot
