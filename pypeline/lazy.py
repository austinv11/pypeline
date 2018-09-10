import shutil
from collections.abc import MutableMapping
from typing import Iterator, Any, Dict

from ._db import *
from . import json


def fs_dict(dir: str) -> Dict[str, Any]:
    return LazyDict(dir)


# Dictionary backed by the filesystem
class LazyDict(MutableMapping[str, Any], Dict[str, Any]):

    def __init__(self, dir):
        super(LazyDict, self).__init__()
        self.dir = dir

    def clear(self):
        shutil.rmtree(self.dir)

    def __setitem__(self, k: str, v: Any) -> None:
        with open_db(self.dir) as db:
            db.put(k.encode(), json.dumps(v).encode())

    def __delitem__(self, v: str) -> None:
        with open_db(self.dir) as db:
            db.delete(v.encode())

    def __getitem__(self, k: str) -> Any:
        with open_db(self.dir) as db:
            return db.get(k.encode())

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
