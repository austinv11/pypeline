"""
This module describes a system for running
"""

from typing import Callable, Any


def _hook_uvloop():
    """
    Attempts wire uvloop.
    """
    import asyncio
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except:
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())


def _ensure_loop_set():
    """
    Ensures that an asyncio event loop exists.
    """
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
    """
    Sets the serializer/deserializer for use by SerializableActions.

    :param serializer: The serializer callback.
    :param deserializer: The deserializer callback.
    """
    global _serializer
    global _deserializer

    _serializer = serializer
    _deserializer = deserializer


from .pypeline import SerializableAction, ResultsHolder, PypelineExecutor, SimplePypelineExecutor, \
    ForkingPypelineExecutor, Pypeline, wrap, extract_fs_kwargs, Action, build_action, SequentialPypelineExecutor

from .off_heap import FileSystemDict

try:
    import ujson as json
except:
    import json

set_serializer(lambda x: json.dumps(x).encode(), lambda x: json.loads(x.decode()))


__all__ = ('SerializableAction', 'ResultsHolder', 'PypelineExecutor', 'SimplePypelineExecutor',
           'ForkingPypelineExecutor', 'Pypeline', 'FileSystemDict', 'wrap', 'extract_fs_kwargs', 'set_serializer',
           'Action', 'build_action', 'SequentialPypelineExecutor')
