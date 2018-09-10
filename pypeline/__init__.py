from typing import Callable, Any

from .pypeline import SerializableAction, ResultsHolder, PypelineExecutor, SimplePypelineExecutor, \
    ForkingPypelineExecutor, Pypeline, wrap, extract_lazy_kwargs

from .lazy import LazyDict


def _hook_uvloop():
    import asyncio
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except:
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())


def _ensure_loop_set():
    import asyncio
    try:
        asyncio.get_event_loop()
    except:
        _hook_uvloop()
        asyncio.set_event_loop(asyncio.get_event_loop_policy().new_event_loop())


_hook_uvloop()


_serializer: Callable[[Any], bytes] = ...
_deserializer: Callable[[bytes], Any] = ...


def set_serializer(serializer: Callable[[Any], bytes], deserializer: Callable[[bytes], Any]):
    global _serializer
    global _deserializer

    _serializer = serializer
    _deserializer = deserializer


try:
    import ujson as json
except:
    import json

set_serializer(lambda x: json.dumps(x).encode(), lambda x: json.loads(x.decode()))


__all__ = ('SerializableAction', 'ResultsHolder', 'PypelineExecutor', 'SimplePypelineExecutor',
           'ForkingPypelineExecutor', 'Pypeline', 'LazyDict', 'wrap', 'extract_lazy_kwargs', 'set_serializer')
