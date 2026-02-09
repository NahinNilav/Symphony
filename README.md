# Symphony

**A production-grade async orchestration framework for multi-agent AI workflows, built from scratch in Python.**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

---

## Why We Built This

The AI agent ecosystem is fragmented. LangChain, CrewAI, Agno, AutoGen — each comes with its own way of chaining agent tasks together, but none of them solve the **orchestration** problem cleanly. If you want to run agents from different frameworks in a single workflow, or swap one framework for another without rewriting your pipeline logic, you're stuck gluing things together with ad-hoc scripts.

We wanted to answer a simple question: **What if the orchestration layer didn't care which agent framework you use?**

Symphony is the result. It's a framework-agnostic orchestrator that treats agent tasks as composable steps — you define *what* each step does, and Symphony handles *when* and *how* they execute. Sequential chains, parallel fan-outs, conditional branching, retry loops — all expressed through a clean builder API that reads like a sentence.

This isn't a wrapper around an existing tool. We designed and implemented every component from the ground up: the execution engine, the type system, the context-passing model, the graph builder, and the API server.

## What We Learned

Building an orchestration engine from scratch forced us to solve real systems-design problems:

| Challenge | What We Did |
|-----------|-------------|
| **Async execution model** | Designed a `Conductor` engine that processes a typed execution graph, dispatching steps through `asyncio.gather` for true concurrency in parallel (ensemble) blocks |
| **Type safety at runtime** | Integrated Pydantic schemas at every step boundary — inputs and outputs are validated before execution, failing fast with clear errors instead of propagating bad data |
| **Context threading** | Built a `Performance` context that flows through the entire pipeline, allowing steps to read prior outputs without tight coupling — essential for multi-step AI workflows where later steps depend on earlier results |
| **Builder pattern design** | Implemented a fluent API (`Pipeline().pipe(a).ensemble([b, c]).fork([...]).seal()`) with an immutable "sealed" state — once a pipeline is sealed, its execution graph is frozen, preventing accidental mutation at runtime |
| **Execution graph compilation** | Pipelines compile into a `Score` (a typed list of `Block` nodes) that the Conductor walks at runtime — separating graph construction from execution makes the system testable and inspectable |
| **Conditional control flow** | Fork blocks evaluate gate functions (sync or async) against live data to route execution dynamically — this is how you build "if the sentiment is negative, escalate to a human" logic |
| **Server layer** | Built a FastAPI-based `Stage` server with auto-generated endpoints for every registered pipeline — inspect, list, and trigger pipelines over HTTP with schema-validated request/response models |

## How It Helps

Symphony solves three real problems in production AI systems:

**1. Framework lock-in is expensive.** Teams adopt LangChain, hit its limitations, and realize rewriting the orchestration layer means rewriting everything. Symphony decouples the "what" (your agent logic) from the "how" (the execution order), so swapping agent frameworks is a step-level change, not a rewrite.

**2. Complex workflows need more than sequential chains.** Real agent systems need parallel execution (query 3 data sources at once), conditional routing (route by classification result), and retry loops (keep calling the LLM until output passes validation). Symphony provides all four patterns through a single composable API.

**3. Multi-agent debugging is hard.** Symphony's `Performance` context records every step's output, input, and execution order in a structured trace. When step 4 of a 6-step pipeline returns bad data, you can inspect exactly what each prior step produced — no print-statement debugging.

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
