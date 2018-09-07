import time
from collections import namedtuple
from typing import Tuple, List

import xxhash
from abc import ABC, abstractmethod

from ._db import *
from . import json


# TODO: Wire context
ResultsHolder = namedtuple("ResultsHolder", ('args', 'kwargs', 'context'))


def build_uid(self: "SerializableAction", *args, **kwargs) -> bytes:
    x = xxhash.xxh64()
    x.update(str(self.task_name).encode())
    for arg in args:
        x.update(str(arg).encode())
    for k, v in kwargs.items():
        x.update((str(k) + str(v)).encode())
    return x.digest()


async def serialize(objs: List[ResultsHolder], procedure_version: str) -> bytes:
    return json.dumps({'payload': [{'args': obj.args, 'kwargs': obj.kwargs, 'context': obj.context} for obj in objs], 'timestamp': time.time(), 'version': procedure_version}, ensure_ascii=False).encode()


async def deserialize(value: bytes) -> dict:
    return json.loads(value.decode())


class SerializableAction(ABC):

    def __init__(self, db_dir: str):
        self.db_dir = db_dir

    @abstractmethod
    @property
    def task_name(self) -> str: ...

    @abstractmethod
    @property
    def version(self) -> str: ...

    @abstractmethod
    async def execute(self, *args, **kwargs) -> List[ResultsHolder]: ...

    async def run(self, *args, **kwargs) -> List[ResultsHolder]:
        uid = build_uid(*args, **kwargs)
        with open_prefixed_db(self.db_dir, uid) as db:
            ret_val = db.get("_".join([self.task_name, uid]))

        if ret_val is None or self.version != ret_val['version']:
            ret_val = await self.execute(*args, **kwargs)
            serialized = await serialize(ret_val, self.version)
            with open_prefixed_db(self.db_dir, uid) as db:
                db.put("_".join([self.task_name, uid]), serialized)
            return ret_val

        ret_val = [ResultsHolder(args=x['args'], kwargs=x['kwargs'], context=x['context']) for x in (await deserialize(ret_val))['payload']]
        return ret_val


class PypelineExecutor(ABC):

    @abstractmethod
    async def run(self, pypeline: 'Pypeline'): ...


class SimplePypelineExecutor(PypelineExecutor):
    """
    Simple sequential executor of pypeline steps. There is very little concurrency so step execution order can be
    considered deterministic.
    """

    async def run(self, pypeline: 'Pypeline') -> List[ResultsHolder]:
        curr_args = None
        for step in pypeline.steps:
            if not curr_args:
                curr_args = await step.run()
            else:
                new_args = []
                for arg_set in curr_args:
                    new_args += await step.run(*arg_set.args, **arg_set.kwargs)
                curr_args = new_args

        return curr_args


class Pypeline:

    def __init__(self):
        self.steps = []

    def add_action(self, action: SerializableAction):
        self.steps.append(action)

    async def run(self, executor: PypelineExecutor = SimplePypelineExecutor()) -> List[ResultsHolder]:
        return await executor.run(self)
