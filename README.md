# Pypeline

This is a package for creating iterative data processing pipelines. Note that
this is NOT a general purpose stream processing library. It is only designed as 
being a low overhead and simple-to-setup stream processing library. So for
large scale production applications, use something like kafka instead.

## Warning
This library is still at an ALPHA stage. So things may not work as intended
and the api is not final!

## Trivial Example
```python
from pypeline import build_action, Pypeline, ForkingPypelineExecutor, wrap
import asyncio

async def step1():
    results = []
    for i in range(1000):
        results.append(wrap(i))
    return results

async def step2(i):
    return i * 10
    
async def step3(i):
    return i + 1

async def run_pipeline():
    pypeline = Pypeline()
    # Adding actions to the pipeline
    pypeline.add_action(build_action("Step1", step1)) \ 
            .add_action(build_action("Step2", step2)) \
            .add_action(build_action("Step3", step3, serialize_dir="./example"))  # Serialize results so future runs will skip this step entirely
    results = await pypeline.run(executor=ForkingPypelineExecutor())  # Custom executor that avoids the GIL
    # Results are wrapped in a utility namedtuple, so let's flatten it.
    results = [r.args[0] for r in results]
    return results
    
results = asyncio.get_event_loop().run_until_complete(run_pipeline())
for result in results:
    print(result)
```