try:
    import ujson as json
except:
    import json

from .pypeline import SerializableAction, ResultsHolder, PypelineExecutor, SimplePypelineExecutor, \
    ForkingPypelineExecutor, Pypeline, wrap


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


__all__ = ('SerializableAction', 'ResultsHolder', 'PypelineExecutor', 'SimplePypelineExecutor',
           'ForkingPypelineExecutor', 'Pypeline', 'wrap')
