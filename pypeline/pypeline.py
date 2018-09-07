import asyncio
import time
from collections import namedtuple, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import List

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

    async def run(self, pypeline: 'Pypeline'):
        curr_args = None
        for step in pypeline.steps:
            if not curr_args:
                curr_args = await step.run()
            else:
                new_args = []
                for arg_set in curr_args:
                    new_args += await step.run(*arg_set.args, **arg_set.kwargs)
                curr_args = new_args


class ForkingPypelineExecutor(PypelineExecutor):

    def __init__(self, max_forking_factor=4):
        self.max_forking_factor = max_forking_factor

    async def run(self, pypeline: 'Pypeline'):
        linked_map = OrderedDict()
        # Convert to a linked(ish) list
        for i, v in enumerate(pypeline.steps):
            if i == len(pypeline.steps) - 1:
                linked_map[v] = None
            else:
                linked_map[v] = pypeline.steps[i+1]

        curr_step = pypeline.steps[0]
        await self.continue_run(linked_map, curr_step)

    async def continue_run(self, linked_map, curr_step, *args, **kwargs):
        results = await curr_step.run(*args, **kwargs)
        forking_factor = min(self.max_forking_factor, len(results))
        curr_step = linked_map[curr_step]

        if curr_step is None or forking_factor == 0:
            return

        if forking_factor == 1:
            def submit_task(coroutine):
                return asyncio.ensure_future(coroutine, loop=asyncio.get_event_loop())
        else:
            pool = ThreadPoolExecutor(max_workers=forking_factor, thread_name_prefix="PypelineExecutor")
            def submit_task(coroutine):
                return asyncio.get_event_loop().run_in_executor(pool, coroutine)

        for result in results:
            coro = self._noarg_coro_builder(curr_step.run, *result.args, **result.kwargs)

            future = submit_task(coro)
            future.add_done_callback(self._callback_builder(curr_step, linked_map))

    def _noarg_coro_builder(self, coro, *args, **kwargs):
        async def coro1(): await coro(*args, **kwargs)
        return coro1

    def _callback_builder(self, next_step, linked_map):
        def callback(future):
            next = next_step
            results = future.result()
            for res in results:
                asyncio.ensure_future(self.continue_run(linked_map, next, *res.args, **res.kwargs))

        return callback


class Pypeline:

    def __init__(self):
        self.steps = []

    def add_action(self, action: SerializableAction):
        self.steps.append(action)

    async def run(self, executor: PypelineExecutor = SimplePypelineExecutor()) -> List[ResultsHolder]:
        return await executor.run(self)
