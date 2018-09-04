import time
import ujson
import xxhash
from abc import ABC, abstractmethod

from ._db import *


def build_uid(self: "SerializableAction", *args, **kwargs) -> bytes:
    x = xxhash.xxh64()
    x.update(str(self.task_name).encode())
    for arg in args:
        x.update(str(arg).encode())
    for k, v in kwargs.items():
        x.update((str(k) + str(v)).encode())
    return x.digest()


async def serialize(obj: object) -> bytes:
    return ujson.encode({'payload': obj, 'timestamp': time.time()}, ensure_ascii=False).encode()


async def deserialize(value: bytes) -> dict:
    return ujson.decode(value.decode())


class SerializableAction(ABC):

    def __init__(self, db_dir: str):
        self.db_dir = db_dir

    @abstractmethod
    @property
    def task_name(self) -> str: ...

    @abstractmethod
    async def execute(self, *args, **kwargs) -> object: ...

    async def run(self, *args, **kwargs) -> object:
        uid = build_uid(*args, **kwargs)
        with open_prefixed_db(self.db_dir, uid) as db:
            ret_val = db.get("_".join([self.task_name, uid]))

        if ret_val is None:
            ret_val = await self.execute(*args, **kwargs)
            serialized = await serialize(ret_val)
            with open_prefixed_db(self.db_dir, uid) as db:
                db.put("_".join([self.task_name, uid]), serialized)
            return ret_val

        return (await deserialize(ret_val))['payload']
