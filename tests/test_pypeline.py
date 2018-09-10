import asyncio
from time import sleep
from typing import List, Any

from pypeline import *


class Start(SerializableAction):

    @property
    def task_name(self) -> str:
        return "Start"

    @property
    def version(self) -> str:
        return "1"

    async def execute(self, *args, **kwargs) -> List[ResultsHolder]:
        results = [x for x in range(10)]
        results = [wrap(x) for x in results]
        return results


class Middle(SerializableAction):

    @property
    def task_name(self) -> str:
        return "Middle"

    @property
    def version(self) -> str:
        return "1"

    async def execute(self, *args, **kwargs) -> List[ResultsHolder]:
        sleep(10)
        return args[0] * 10  # Tests implicit coercion to List[ResultsHolder]


class End(SerializableAction):

    @property
    def task_name(self) -> str:
        return "End"

    @property
    def version(self) -> str:
        return "2"

    async def post_execute(self, return_value: List[ResultsHolder], *args, **kwargs) -> Any:
        print(args[0])
        return return_value

    async def execute(self, *args, **kwargs) -> List[ResultsHolder]:
        return [wrap(args[0])]


def test():
    pypeline = Pypeline()
    pypeline.add_action(Start("./test_db"))
    pypeline.add_action(Middle("./test_db"))
    pypeline.add_action(End("./test_db"))
    results = asyncio.get_event_loop().run_until_complete(pypeline.run(executor=ForkingPypelineExecutor()))
    print(results)


if __name__ == '__main__':
    test()
