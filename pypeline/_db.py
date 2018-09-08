import os
from contextlib import contextmanager

import plyvel


@contextmanager
def open_db(loc: str) -> plyvel.DB:
    from filelock import FileLock

    lock = FileLock(os.path.join(loc, "LOCK.lock"))
    with lock:
        db = plyvel.DB(loc, create_if_missing=True)
        yield db
        db.close()


@contextmanager
def open_prefixed_db(loc: str, prefix: bytes) -> plyvel.DB:
    with open_db(loc) as db:
        yield db.prefixed_db(prefix)
