import asyncio
import time
from collections import namedtuple, OrderedDict
from multiprocess.pool import Pool
from typing import List, Tuple, Dict, Any

import xxhash
from abc import ABC, abstractmethod

from ._db import *
from . import json


# TODO: Wire context
ResultsHolder = namedtuple("ResultsHolder", ('args', 'kwargs', 'context'))


def wrap(*args, **kwargs) -> ResultsHolder:
    return ResultsHolder(args, kwargs, {})


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

    @property
    @abstractmethod
    def task_name(self) -> str: ...

    @property
    @abstractmethod
    def version(self) -> str: ...

    async def pre_execute(self, *args, **kwargs) -> Tuple[Tuple, Dict]: return args, kwargs

    async def post_execute(self, return_value: List[ResultsHolder], *args, **kwargs) -> Any: return return_value

    @abstractmethod
    async def execute(self, *args, **kwargs) -> List[ResultsHolder]: ...

    async def run(self, *args, **kwargs) -> List[ResultsHolder]:
        uid = build_uid(self, *args, **kwargs)
        key = self.task_name.encode() + b'_' + uid
        with open_prefixed_db(self.db_dir, uid) as db:
            ret_val = db.get(key)

        if ret_val is not None:
            ret_val = await deserialize(ret_val)

        if ret_val is None or self.version != ret_val['version']:
            should_remove = ret_val is not None
            args, kwargs = await self.pre_execute(*args, **kwargs)
            ret_val = await self.execute(*args, **kwargs)
            ret_val = await self.post_execute(ret_val, *args, **kwargs)
            serialized = await serialize(ret_val, self.version)
            with open_prefixed_db(self.db_dir, uid) as db:
                if should_remove:
                    db.delete(key)
                db.put(key, serialized)
            return ret_val

        args, kwargs = await self.pre_execute(*args, **kwargs)
        ret_val = [ResultsHolder(args=x['args'], kwargs=x['kwargs'], context=x['context']) for x in ret_val['payload']]
        await self.post_execute(ret_val, *args, **kwargs)
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

    def __init__(self, max_forking_factor=1):
        assert max_forking_factor < 2  # Forking isn't supported very well yet
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
            for result in results:
                coro = ForkingPypelineExecutor._noarg_coro_builder(self.continue_run, linked_map, curr_step, *result.args, **result.kwargs)

                asyncio.ensure_future(coro, loop=asyncio.get_event_loop())
        else:
            # FIXME
            pool = Pool(processes=forking_factor)

            for result in results:
                pool.apply_async(ForkingPypelineExecutor._forked_process(curr_step, *result.args, **result.kwargs), callback=self._callback_builder(curr_step, linked_map)).get()

    @staticmethod
    def _forked_process(step, *args, **kwargs):
        def _inner_forked_process():
            return asyncio.wait_for(ForkingPypelineExecutor._coro_builder(step, *args, **kwargs), 10000000000)
        return _inner_forked_process

    @staticmethod
    def _coro_builder(step, *args, **kwargs):
        return step.run(*args, **kwargs)

    @staticmethod
    def _noarg_coro_builder(coro, linked_map, curr_step, *args, **kwargs):
        async def coro1(): return await coro(linked_map, curr_step, *args, **kwargs)
        return coro1()

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
