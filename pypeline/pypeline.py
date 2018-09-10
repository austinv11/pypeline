import asyncio
import time
from collections import namedtuple
from multiprocess.pool import Pool
from typing import List, Tuple, Optional

import xxhash
from abc import ABC, abstractmethod

from ._db import *
from . import _serializer, _deserializer
from .lazy import *


# TODO: Wire context
ResultsHolder = namedtuple("ResultsHolder", ('args', 'kwargs', 'context'))


def wrap(*args, **kwargs) -> ResultsHolder:
    return ResultsHolder(args, kwargs, {})


def extract_lazy_kwargs(**kwargs) -> Optional[LazyDict]:
    for v in kwargs.values():
        if isinstance(v, LazyDict):
            return v

    return None


def build_uid(self: "SerializableAction", *args, **kwargs) -> bytes:
    x = xxhash.xxh64()
    x.update(str(self.task_name).encode())
    for arg in args:
        x.update(str(arg).encode())
    for k, v in kwargs.items():
        x.update((str(k) + str(v)).encode())
    return x.digest()


async def serialize(objs: List[ResultsHolder], procedure_version: str) -> bytes:
    return _serializer({'payload': [{'args': obj.args, 'kwargs': obj.kwargs, 'context': obj.context} for obj in objs], 'timestamp': time.time(), 'version': procedure_version}, ensure_ascii=False).encode()


async def deserialize(value: bytes) -> dict:
    return _deserializer(value.decode())


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


class _ForkingSlave:

    def __init__(self, callable):
        self.callable = callable

    async def run(self):
        coro = asyncio.coroutine(self.callable)
        child = await coro()
        if child:
            await child.run()


class ForkingPypelineExecutor(PypelineExecutor):

    def __init__(self, max_forking_factor=max((os.cpu_count() or 0) // 2, 2)):
        self.max_forking_factor = max_forking_factor

    @staticmethod
    def _callable_packer(delegate, steps, max_forking_factor, index, *args, **kwargs):
        def _callable():
            return delegate(steps, max_forking_factor, index, *args, **kwargs)
        return _callable

    @staticmethod
    def _make_slave(callable):
        return _ForkingSlave(callable)

    @staticmethod
    def __child_process(steps, max_forking_factor, index, results):
        from . import _ensure_loop_set
        _ensure_loop_set()
        loop = asyncio.new_event_loop()
        coros = []
        for result in results:
            coros.append(ForkingPypelineExecutor.__callable(steps, max_forking_factor, index, *result.args,
                                                            **result.kwargs))
        return loop.run_until_complete(asyncio.gather(*coros, loop=loop))

    @staticmethod
    async def __callable(steps, max_forking_factor, index, *args, **kwargs):
        if index >= len(steps):
            return

        step = steps[index]
        results = await step.run(*args, **kwargs)
        forking_factor = min(max_forking_factor, len(results))

        if forking_factor == 0:
            return

        if forking_factor == 1:
            coros = []
            for res in results:
                coros.append(ForkingPypelineExecutor.__callable(steps, max_forking_factor, index+1, *res.args,
                                                                **res.kwargs))
            return ForkingPypelineExecutor._make_slave(asyncio.gather(*coros))
        else:
            children = []
            for res in results:
                children.append(ForkingPypelineExecutor._callable_packer(ForkingPypelineExecutor.__child_process,
                                                                         steps, max_forking_factor, index+1,
                                                                         results=[res]))

            async def waiter():
                def _call(child):
                    child()
                pool = Pool(forking_factor)
                pool.map(_call, children)
                pool.close()
                pool.join()

            return ForkingPypelineExecutor._make_slave(waiter)

    async def run(self, pypeline: 'Pypeline'):
        slave = ForkingPypelineExecutor._make_slave(
            ForkingPypelineExecutor._callable_packer(ForkingPypelineExecutor.__callable,
                                                     pypeline.steps,
                                                     self.max_forking_factor,
                                                     0))

        while slave:
            slave = await slave.run()


class Pypeline:

    def __init__(self):
        self.steps = []

    def add_action(self, action: SerializableAction):
        self.steps.append(action)

    async def run(self, executor: PypelineExecutor = SimplePypelineExecutor()) -> List[ResultsHolder]:
        return await executor.run(self)
