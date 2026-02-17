# Symphony

**A production-grade async orchestration framework for multi-agent AI workflows, built from scratch in Python.**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

---

### Why I Built Symphony

The AI agent ecosystem is fragmented. Chaining tools like LangChain or CrewAI together across frameworks usually requires messy, ad-hoc glue code.

I built **Symphony** to answer a simple question: **What if the orchestration layer didn't care which agent framework you use?**

Symphony is a framework-agnostic orchestrator built entirely from scratch. You define *what* your agents do, and Symphony’s clean builder API handles *when* and *how* they execute—seamlessly managing sequential chains, parallel fan-outs, and conditional routing without tying you to a single tool.

## What I Learned

Building an orchestration engine from scratch forced me to solve real systems-design problems:

| Challenge | How I Solved It |
|-----------|-----------------|
| **Async execution** | Built a `Conductor` engine using `asyncio.gather` for true parallel concurrency. |
| **Runtime type safety** | Integrated Pydantic at every step to validate data and fail fast. |
| **Context threading** | Created a `Performance` context so later steps can cleanly read prior outputs. |
| **Builder pattern** | Designed a fluent API (`Pipeline().pipe(a).seal()`) that freezes state to prevent bugs. |
| **Graph compilation** | Compiled pipelines into a testable `Score`, separating construction from execution. |
| **Dynamic routing** | Built `Fork` blocks to evaluate live data and route execution on the fly. |
| **Server layer** | Added a FastAPI `Stage` server with auto-generated endpoints to trigger pipelines. |

## How It Helps

Symphony solves three major bottlenecks in production AI systems:

**1. Prevents framework lock-in.** Symphony decouples the *what* (agent logic) from the *how* (execution order). Swapping frameworks is now a simple step-level tweak, not a full rewrite.

**2. Handles complex workflows natively.** Production requires parallel execution, conditional routing, and retry loops. Symphony provides all of these out of the box through a single, composable API.

**3. Makes debugging painless.** The `Performance` context records every input, output, and step in a structured trace. If a pipeline fails, you instantly see exactly what prior steps produced—no more `print()` debugging.
## Architecture

```
Pipeline (builder)
    │
    ▼
Score (execution graph: list of typed Blocks)
    │
    ▼
Conductor (async engine)
    │
    ├── SerialBlock    → steps run one after another
    ├── EnsembleBlock  → steps run concurrently via asyncio.gather
    ├── ForkBlock      → gate functions route to one step
    └── CycleBlock     → step repeats until gate returns False
    │
    ▼
Performance (context: carries state + step outputs across the pipeline)
```

## Quick Start

```bash
pip install symphony-orc
```

```python
import asyncio
from symphony import Pipeline, create_step
from pydantic import BaseModel

class Input(BaseModel):
    value: int

class Output(BaseModel):
    result: int

def double(params, perf):
    return {"result": params["payload"]["value"] * 2}

step = create_step(
    id="double",
    description="Doubles a number",
    input_schema=Input,
    output_schema=Output,
    handler=double
)

pipeline = Pipeline(id="math", description="Simple math").pipe(step).seal()

async def main():
    result = await pipeline.perform({"value": 21})
    print(result)  # {"result": 42}

asyncio.run(main())
```

## Serving Pipelines over HTTP

Symphony includes a built-in `Stage` server that exposes all sealed pipelines as REST endpoints:

```python
from symphony import Pipeline, Stage

stage = Stage(pipelines=[registration_pipeline, notification_pipeline])
stage.run(port=8000)
```


## License

Apache License 2.0 — see [LICENSE](LICENSE).
