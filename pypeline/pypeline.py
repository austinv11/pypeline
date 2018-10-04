"""
Main library functionality.
"""

import asyncio
import itertools
import time
from collections import namedtuple, Iterable
from multiprocess.pool import Pool
from typing import List, Tuple, Optional, Callable, Awaitable

import xxhash
from abc import ABC, abstractmethod

from ._db import *
from .off_heap import *

# TODO: Wire context

"""
This is used to pass args to further steps in the pypeline. Note that context is currently NO-OP and may be removed
entirely.
"""
ResultsHolder = namedtuple("ResultsHolder", ('args', 'kwargs', 'context'))


def wrap(*args, **kwargs) -> ResultsHolder:
    """
    Convenience method for generating ResultsHolders.

    :param args: Arguments to convert.
    :param kwargs: Keyword arguments to convert.
    :return: The generated holder object.
    """
    return ResultsHolder(args, kwargs, {})


def _coerce(ret_val: Any) -> List[ResultsHolder]:
    """
    This is an internal helper to ensure that action steps return results in the form of List[ResultsHolder].
    :param ret_val: The value to double check.
    :return: The corrected return value.
    """
    if ret_val is None:
        return []

    if isinstance(ret_val, list):
        if len(ret_val) == 0:
            return ret_val
        elif isinstance(ret_val[0], ResultsHolder):
            return ret_val
        else:
            return list(itertools.chain(*[_coerce(x) for x in ret_val]))
    else:
        if isinstance(ret_val, ResultsHolder):
            return [ret_val]
        else:
            return [wrap(ret_val)]


def extract_fs_kwargs(**kwargs) -> Optional[FileSystemDict]:
    """
    Searches a set of keyword arguments to find one which is a FileSystemDict
    :param kwargs: The kwargs to search.
    :return: The FileSystemDict if found, else None.
    """
    for v in kwargs.values():
        if isinstance(v, FileSystemDict):
            return v

    return None


def build_uid(self: "SerializableAction", *args, **kwargs) -> bytes:
    """
    Generates a UID based on XXHash64 from a SerializableAction and its arguments for storage in the backing filesystem
     database.
    :param self: The action.
    :param args: The positional args.
    :param kwargs: The keyword args.
    :return: The UID.
    """
    x = xxhash.xxh64()
    x.update(str(self.task_name).encode())
    for arg in args:
        x.update(str(arg).encode())
    for k, v in kwargs.items():
        x.update((str(k) + str(v)).encode())
    return x.digest()


async def serialize(objs: List[ResultsHolder], procedure_version: str) -> bytes:
    """
    Wrapper for serializing results.
    :param objs: The results to serialize.
    :param procedure_version: The version of the action.
    :return: The serialized payload.
    """
    from . import _serializer
    return _serializer({'payload': [{'args': obj.args, 'kwargs': obj.kwargs, 'context': obj.context} for obj in objs], 'timestamp': time.time(), 'version': procedure_version})


async def deserialize(value: bytes) -> dict:
    """
    Wrapper for deserializing results.
    :param value: The serialized results.
    :return: The deserialized results.
    """
    from . import _deserializer
    return _deserializer(value)


class Action(ABC):
    """
    This represents an action to be run in a pypeline.
    """

    @property
    @abstractmethod
    def task_name(self) -> str:
        """
        The name of the action. This should be unique as it seeds internal hashing mechanisms!
        :return: The action name.
        """
        ...

    @property
    @abstractmethod
    def version(self) -> str:
        """
        The version of the action. This is important for allowing for a pypeline to be updated on-the-fly. Pypeline
        should automatically attempt to update your results based on this.
        :return: The version, this should be modified everytime your run() function gets updated.
        """
        ...

    @abstractmethod
    async def run(self, *args, **kwargs) -> List[ResultsHolder]:
        """
        This is a coroutine which gets called to execute this step.
        :param args: The positional arguments from the past action.
        :param kwargs: The keyword arguments from the past action.
        :return: The list of results, each represents a new step to be ran. Note: While it is recommended to return a
            value of type List[ResultsHolder], Pypeline will attempt to implicitly convert other return values (but
            there is no guarantee that it will work as intended!).
        """
        ...


class SerializableAction(Action, ABC):
    """
    This represents an action which has its results serialized in order to speed up compute power in the future.
    """

    def __init__(self, db_dir: str):
        if not is_plyvel_installed():
            raise ImportError("This is unusable if plyvel is not installed!")

        """
        :param db_dir: The directory for the LevelDB database to be held.
        """
        self.db_dir = db_dir

    @property
    @abstractmethod
    def task_name(self) -> str: ...

    @property
    @abstractmethod
    def version(self) -> str: ...

    async def pre_execute(self, *args, **kwargs) -> Tuple[Tuple, Dict]:
        """
        Optional override. This is ALWAYS called before the main execute() coroutine is invoked.

        :param args: The positional arguments passed to this action.
        :param kwargs: The keyword arguments passed to this action.
        :return: The modified positional and keyword arguments for this action to use.
        """
        return args, kwargs

    async def post_execute(self, return_value: List[ResultsHolder], *args, **kwargs) -> Any:
        """
        Optional override. This is ALWAYS called after the main execute() coroutine is invoked.

        :param return_value: The original return value of this action.
        :param args: The positional arguments passed to this action.
        :param kwargs: The keyword arguments passed to this action.
        :return: The modified return value of this action.
        """
        return return_value

    @abstractmethod
    async def execute(self, *args, **kwargs) -> List[ResultsHolder]:
        """
        This method supercedes the standard run() coroutine.

        :param args: The positional arguments passed to this action.
        :param kwargs: The keyword arguments passed to this action.
        :return: The results of this action.
        """
        ...

    async def run(self, *args, **kwargs) -> List[ResultsHolder]:
        """
        Do NOT override this!
        """
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
    """
    This represents an abstract executor of pypeline actions.
    """

    @abstractmethod
    async def run(self, pypeline: 'Pypeline') -> List[ResultsHolder]:
        """
        This is invoked to run the given pypeline from start to finish.
        :param pypeline: The pypeline to run.
        :return: The latest results.
        """
        ...


class SequentialPypelineExecutor(PypelineExecutor):
    """
    Simple sequential executor of pypeline steps. There is very little concurrency so step execution order can be
    considered deterministic.
    """

    async def run(self, pypeline: 'Pypeline') -> List[ResultsHolder]:
        curr_args = None
        for step in pypeline.steps:
            if not curr_args:
                curr_args = _coerce(await step.run())
            else:
                new_args = []
                for arg_set in curr_args:
                    new_args.append(_coerce(await step.run(*arg_set.args, **arg_set.kwargs)))
                flattened_results = list(itertools.chain(*new_args))
                curr_args = flattened_results

        return curr_args


class SimplePypelineExecutor(PypelineExecutor):
    """
    Simple event loop-bound executor.
    """

    async def run(self, pypeline: 'Pypeline') -> List[ResultsHolder]:
        curr_args = None
        for step in pypeline.steps:
            if not curr_args:
                curr_args = _coerce(await step.run())
            else:
                coros = []
                for arg_set in curr_args:
                    coros.append(step.run(*arg_set.args, **arg_set.kwargs))
                total_coro = asyncio.gather(*coros)
                results = await total_coro
                curr_args = _coerce(results)

        return curr_args


class _ForkingSlave:
    """
    Internal helper for ForkingPypelineExecutor.
    """

    def __init__(self, callable):
        """
        :param callable: The callable this slave invokes.
        """
        self.callable = callable

    async def run(self) -> List[ResultsHolder]:
        if not asyncio.iscoroutine(self.callable) and not asyncio.isfuture(self.callable):
            coro = asyncio.coroutine(self.callable)
        else:
            coro = self.callable
        if asyncio.iscoroutine(coro) or asyncio.isfuture(coro):
            children = await coro
        else:
            children = await coro()

        return _coerce(children)


class _InstantReturningForkingSlave(_ForkingSlave):
    """
    Internal helper for ForkingPypelineExecutor.
    """

    def __init__(self, results: List[ResultsHolder]):
        self.results = results

    async def run(self) -> List[ResultsHolder]:
        return _coerce(self.results)


class ForkingPypelineExecutor(PypelineExecutor):
    """
    Totally concurrent executor. This spawns threads based on the passed max_forking_factor on every step execution in
    order to run every step independently of the Python GIL. This is not a very scalable executor! For very long
    pypelines, this can lead to unbounded thread creation which may actually end up slowing down your processing!
    """

    def __init__(self, max_forking_factor=max((os.cpu_count() or 0) // 2, 2)):
        """
        :param max_forking_factor: The maximum amount of threads each run step can spawn. The number of threads which
        will be spawned is in the order of n^m where n=max_forking_factor and m=total number of pypeline steps.
        """
        self.max_forking_factor = max_forking_factor

    @staticmethod
    def _callable_packer(delegate, steps, max_forking_factor, index, *args, **kwargs):
        """Internal utility"""
        def _callable():
            return delegate(steps, max_forking_factor, index, *args, **kwargs)
        return _callable

    @staticmethod
    def _make_slave(callable):
        """Internal utility"""
        if isinstance(callable, list):
            return _InstantReturningForkingSlave(callable)
        elif isinstance(callable, ResultsHolder):
            return _InstantReturningForkingSlave([callable])
        else:
            return _ForkingSlave(callable)

    @staticmethod
    def __child_process(steps, max_forking_factor, index, results):
        """Internal function called on generated threads"""
        from . import _ensure_loop_set
        _ensure_loop_set()
        loop = asyncio.new_event_loop()
        coros = []
        for result in results:
            coros.append(ForkingPypelineExecutor.__callable(steps, max_forking_factor, index, *result.args,
                                                            **result.kwargs))

        async def _invoke():
            return await asyncio.gather(*coros, loop=loop)

        return loop.run_until_complete(_invoke())

    @staticmethod
    async def __callable(steps, max_forking_factor, index, *args, **kwargs):
        """Handles the current step in the current thread"""
        if index >= len(steps):
            return await ForkingPypelineExecutor._make_slave(wrap(*args, **kwargs)).run()

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
            return await ForkingPypelineExecutor._make_slave(asyncio.gather(*coros)).run()
        else:
            children = []
            for res in results:
                children.append(ForkingPypelineExecutor._callable_packer(ForkingPypelineExecutor.__child_process,
                                                                         steps, max_forking_factor, index+1,
                                                                         results=[res]))

            async def waiter():
                def _call(child):
                    return child()
                pool = Pool(forking_factor)
                results = pool.map(_call, children)
                pool.close()
                pool.join()
                return results

            return await ForkingPypelineExecutor._make_slave(waiter).run()

    async def run(self, pypeline: 'Pypeline') -> List[ResultsHolder]:
        slave = ForkingPypelineExecutor._make_slave(
            ForkingPypelineExecutor._callable_packer(ForkingPypelineExecutor.__callable,
                                                     pypeline.steps,
                                                     self.max_forking_factor,
                                                     0))

        return await slave.run()


class Pypeline:
    """
    Manager of actions.
    """

    def __init__(self):
        self.steps = []

    def add_action(self, action: Action) -> "Pypeline":
        """
        Adds an action to this pypeline.
        :param action: The action to add.
        :return: The current pypeline, this is for chaining methods.
        """
        self.steps.append(action)
        return self

    async def run(self, executor: PypelineExecutor = SimplePypelineExecutor()) -> List[ResultsHolder]:
        """
        A coroutine which represents the execution of the entire pypeline.
        :param executor: The executor for the pypeline.
        :return: The results generated by the final step of the pypeline.
        """
        return await executor.run(_FrozenPypeline(self))


class _FrozenPypeline(Pypeline):
    """
    Internal helper, attempts to freeze the steps of the backing pypeline.
    """

    def __init__(self, pypeline: Pypeline):
        super().__init__()
        self.steps = tuple(pypeline.steps)  # Freeze the steps

    def add_action(self, action: Action) -> "Pypeline":
        # TODO: Warning message?
        return self

    async def run(self, executor: PypelineExecutor = SimplePypelineExecutor()) -> List[ResultsHolder]:
        return await super().run(executor)


async def _default_pre_run(*args, **kwargs):
    """Internal default helper"""
    return args, kwargs


async def _default_post_run(results, *args, **kwargs):
    """Internal default helper"""
    return results


def build_action(task_name: str,
                 runnable: Callable[[Tuple, Dict], Awaitable[List[ResultsHolder]]],
                 version: str = "0",
                 pre_run: Callable[[Tuple, Dict], Awaitable[Tuple[Tuple, Dict]]] = _default_pre_run,
                 post_run: Callable[[List[ResultsHolder], Tuple, Dict], Awaitable[List[ResultsHolder]]] = _default_post_run,
                 serialize_dir: Optional[str] = None) -> Action:
    """
    Builds an action. Implementing a class can be verbose so this is an alternative.
    :param task_name: The name of the task (this should try to be as unique as possible!).
    :param runnable: The function to invoke to create a coroutine for this task to run.
    :param version: The version of the action.
    :param pre_run: A coroutine generator invoked before an action is run to modify the passed args.
    :param post_run: A coroutine generator invoked after an action is run to modify the returned value.
    :param serialize_dir: The directory to cache the results of this action. If this is None, this action is not \
        serialized.
    :return: The built action.
    """

    if serialize_dir:

        class MySerializableAction(SerializableAction):

            @property
            def task_name(self) -> str:
                return task_name

            @property
            def version(self) -> str:
                return version

            async def pre_execute(self, *args, **kwargs) -> Tuple[Tuple, Dict]:
                return await pre_run(*args, **kwargs)

            async def post_execute(self, return_value: List[ResultsHolder], *args, **kwargs) -> Any:
                return await post_run(return_value, *args, **kwargs)

            async def execute(self, *args, **kwargs) -> List[ResultsHolder]:
                return await runnable(*args, **kwargs)

        return MySerializableAction(serialize_dir)

    else:

        class MyAction(Action):

            @property
            def task_name(self) -> str:
                return task_name

            @property
            def version(self) -> str:
                return version

            async def run(self, *args, **kwargs) -> List[ResultsHolder]:
                args, kwargs = await pre_run(*args, **kwargs)
                results = await runnable(*args, **kwargs)
                results = await post_run(results, *args, **kwargs)
                return results

        return MyAction()
