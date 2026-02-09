import pytest
import asyncio
from pydantic import BaseModel
from symphony import create_step
from symphony.pipeline import Pipeline


# --- Test Schemas ---

class NumberInput(BaseModel):
    value: int

class NumberOutput(BaseModel):
    value: int

# --- Pipeline Initialization Tests ---

def test_pipeline_auto_generated_id():
    pipeline = Pipeline(id="test_pipeline", description="Test pipeline with auto-generated ID")
    assert pipeline.id == "test_pipeline"
    assert pipeline.description == "Test pipeline with auto-generated ID"

def test_pipeline_custom_id_description():
    pipeline = Pipeline(id="custom_pipeline", description="Custom pipeline with explicit description")
    assert pipeline.id == "custom_pipeline"
    assert pipeline.description == "Custom pipeline with explicit description"

# --- Step Validation Tests ---

def test_pipe_none_step():
    pipeline = Pipeline(id="test_pipeline", description="Test pipeline for None step")
    with pytest.raises(ValueError, match="Step cannot be None"):
        pipeline.pipe(None)

def test_empty_pipeline_sealing():
    pipeline = Pipeline(id="test_pipeline", description="Test pipeline for empty sealing")
    with pytest.raises(ValueError, match="Pipeline must have at least one step"):
        pipeline.seal()

# --- Tests ---

@pytest.mark.asyncio
async def test_serial_pipeline_success():
    add_one = create_step(
        id="add_one",
        description="Add one to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    double = create_step(
        id="double",
        description="Double the input value",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )

    pipeline = Pipeline(id="simple_pipeline", description="Add one, then double")
    pipeline.pipe(add_one).pipe(double).seal()

    result = await pipeline.perform({"value": 3})
    assert result["value"] == 8  # (3 + 1) * 2

@pytest.mark.asyncio
async def test_pipeline_requires_sealing():
    add_one = create_step(
        id="add_one",
        description="Add one to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    pipeline = Pipeline(id="unsealed_pipeline", description="Pipeline that should fail without sealing")
    pipeline.pipe(add_one)
    with pytest.raises(RuntimeError, match="Pipeline must be sealed before performing"):
        await pipeline.perform({"value": 1})

def test_cannot_add_after_seal():
    add_one = create_step(
        id="add_one",
        description="Add one to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    pipeline = Pipeline(id="no_add_after_seal", description="Test adding after sealing")
    pipeline.pipe(add_one).seal()
    with pytest.raises(RuntimeError, match="Cannot add steps after sealing"):
        pipeline.pipe(add_one)

@pytest.mark.asyncio
async def test_complex_pipeline_with_regular_functions():
    def multiply_and_add(params, perf):
        v = params["payload"]["value"]
        return {"value": v * 2 + 5}

    def subtract_three(params, perf):
        v = params["payload"]["value"]
        return {"value": v - 3}

    step1 = create_step(
        id="multiply_add",
        description="Multiply by 2 and add 5",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=multiply_and_add
    )

    step2 = create_step(
        id="subtract_three",
        description="Subtract 3 from the result",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=subtract_three
    )

    pipeline = Pipeline(id="complex_pipeline", description="Multiply/add, then subtract")
    pipeline.pipe(step1).pipe(step2).seal()

    result = await pipeline.perform({"value": 4})
    assert result["value"] == 10  # (4*2+5)=13, 13-3=10

@pytest.mark.asyncio
async def test_forking_pipeline():
    def high_handler(params, perf):
        return {"value": params["payload"]["value"] * 2}

    def low_handler(params, perf):
        return {"value": params["payload"]["value"] + 1}

    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )

    step2 = create_step(
        id="high_step",
        description="Double the value for high inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=high_handler
    )

    step3 = create_step(
        id="low_step",
        description="Add 1 for low inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=low_handler
    )

    pipeline = Pipeline(id="forking_pipeline", description="Test forking logic")
    pipeline.pipe(step1).fork([
        (lambda data: data["value"] > 10, step2),
        (lambda data: data["value"] <= 10, step3)
    ]).seal()

    # Test high fork
    result = await pipeline.perform({"value": 8})
    assert result["value"] == 26  # (8 + 5) > 10, so high_step: 13 * 2

    # Test low fork
    result = await pipeline.perform({"value": 2})
    assert result["value"] == 8  # (2 + 5) <= 10, so low_step: 7 + 1

@pytest.mark.asyncio
async def test_empty_fork_list():
    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    pipeline = Pipeline(id="empty_fork_pipeline", description="Test empty fork list")
    with pytest.raises(ValueError, match="Fork list cannot be empty"):
        pipeline.pipe(step1).fork([]).seal()

@pytest.mark.asyncio
async def test_no_matching_fork():
    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    step2 = create_step(
        id="high_step",
        description="Double the value for high inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )
    step3 = create_step(
        id="low_step",
        description="Add 1 for low inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    step4 = create_step(
        id="final_step",
        description="Add 5 to the final value",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    pipeline = Pipeline(id="no_match_pipeline", description="Test no matching fork")
    pipeline.pipe(step1).fork([
        (lambda data: data["value"] > 100, step2),
        (lambda data: data["value"] < 0, step3)
    ]).pipe(step4).seal()
    result = await pipeline.perform({"value": 50})
    assert result["value"] == 60  # step1: 50 + 5, then step4: 55 + 5

@pytest.mark.asyncio
async def test_async_fork_gate():
    async def async_gate(data):
        await asyncio.sleep(0.01)
        return data["value"] > 10

    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    step2 = create_step(
        id="high_step",
        description="Double the value for high inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )
    step3 = create_step(
        id="low_step",
        description="Add 1 for low inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    pipeline = Pipeline(id="async_fork_pipeline", description="Test async fork gate")
    with pytest.raises(ValueError, match="Fork gates cannot be async functions"):
        pipeline.pipe(step1).fork([
            (async_gate, step2),
            (lambda data: data["value"] <= 10, step3)
        ]).seal()

@pytest.mark.asyncio
async def test_fork_gate_exception():
    def failing_gate(data):
        raise RuntimeError("Gate failed")

    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    step2 = create_step(
        id="high_step",
        description="Double the value for high inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )
    step3 = create_step(
        id="low_step",
        description="Add 1 for low inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    pipeline = Pipeline(id="fork_exception_pipeline", description="Test fork gate exception")
    pipeline.pipe(step1).fork([
        (failing_gate, step2),
        (lambda data: data["value"] <= 10, step3)
    ]).seal()
    with pytest.raises(RuntimeError, match="Gate failed"):
        await pipeline.perform({"value": 8})

@pytest.mark.asyncio
async def test_fork_step_exception():
    failing_step = create_step(
        id="failing_step",
        description="Step that raises an exception",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: (_ for _ in ()).throw(RuntimeError("Step failed"))
    )

    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    step3 = create_step(
        id="low_step",
        description="Add 1 for low inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    pipeline = Pipeline(id="fork_step_exception_pipeline", description="Test fork step exception")
    pipeline.pipe(step1).fork([
        (lambda data: data["value"] > 10, failing_step),
        (lambda data: data["value"] <= 10, step3)
    ]).seal()
    with pytest.raises(RuntimeError, match="Step failed"):
        await pipeline.perform({"value": 8})

@pytest.mark.asyncio
async def test_fork_invalid_step():
    step1 = create_step(
        id="initial_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    step3 = create_step(
        id="low_step",
        description="Add 1 for low inputs",
        input_schema=NumberOutput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    pipeline = Pipeline(id="invalid_step_pipeline", description="Test fork with invalid step")
    with pytest.raises(ValueError, match="Step cannot be None"):
        pipeline.pipe(step1).fork([
            (lambda data: data["value"] > 10, None),
            (lambda data: data["value"] <= 10, step3)
        ]).seal()

@pytest.mark.asyncio
async def test_ensemble_execution():
    add_step = create_step(
        id="add_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    
    multiply_step = create_step(
        id="multiply_step",
        description="Multiply input by 2",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )
    
    pipeline = Pipeline(id="ensemble_pipeline", description="Test ensemble step execution")
    pipeline.ensemble([add_step, multiply_step]).seal()
    
    result = await pipeline.perform({"value": 10})
    
    assert "add_step" in result
    assert "multiply_step" in result
    assert result["add_step"]["value"] == 15  # 10 + 5
    assert result["multiply_step"]["value"] == 20  # 10 * 2

@pytest.mark.asyncio
async def test_ensemble_with_async_steps():
    async def async_add(params, perf):
        await asyncio.sleep(0.1)
        return {"value": params["payload"]["value"] + 5}
    
    async def async_multiply(params, perf):
        await asyncio.sleep(0.1)
        return {"value": params["payload"]["value"] * 2}
    
    add_step = create_step(
        id="async_add_step",
        description="Async add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=async_add
    )
    
    multiply_step = create_step(
        id="async_multiply_step",
        description="Async multiply input by 2",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=async_multiply
    )
    
    pipeline = Pipeline(id="async_ensemble_pipeline", description="Test ensemble async step execution")
    pipeline.ensemble([add_step, multiply_step]).seal()
    
    start_time = asyncio.get_event_loop().time()
    result = await pipeline.perform({"value": 10})
    end_time = asyncio.get_event_loop().time()
    
    assert "async_add_step" in result
    assert "async_multiply_step" in result
    assert result["async_add_step"]["value"] == 15  # 10 + 5
    assert result["async_multiply_step"]["value"] == 20  # 10 * 2
    
    execution_time = end_time - start_time
    assert execution_time < 0.15  # Verify ensemble ran concurrently

@pytest.mark.asyncio
async def test_ensemble_with_different_output_keys():
    step1 = create_step(
        id="sum_step",
        description="Calculate sum",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"sum": params["payload"]["value"] + 5}
    )
    
    step2 = create_step(
        id="product_step",
        description="Calculate product",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"product": params["payload"]["value"] * 2}
    )
    
    pipeline = Pipeline(id="different_keys_pipeline", description="Test ensemble steps with different output keys")
    pipeline.ensemble([step1, step2]).seal()
    
    result = await pipeline.perform({"value": 10})
    
    assert "sum_step" in result
    assert "product_step" in result
    assert result["sum_step"]["sum"] == 15  # 10 + 5
    assert result["product_step"]["product"] == 20  # 10 * 2

@pytest.mark.asyncio
async def test_empty_ensemble_list():
    pipeline = Pipeline(id="empty_ensemble_pipeline", description="Test empty ensemble step list")
    with pytest.raises(ValueError, match="Ensemble step list cannot be empty"):
        pipeline.ensemble([]).seal()

@pytest.mark.asyncio
async def test_ensemble_with_none_step():
    add_step = create_step(
        id="add_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    
    pipeline = Pipeline(id="none_ensemble_pipeline", description="Test ensemble with None step")
    with pytest.raises(ValueError, match="Step cannot be None"):
        pipeline.ensemble([add_step, None]).seal()

@pytest.mark.asyncio
async def test_ensemble_step_exception():
    failing_step = create_step(
        id="failing_step",
        description="Step that raises an exception",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: (_ for _ in ()).throw(RuntimeError("Step failed"))
    )
    
    add_step = create_step(
        id="add_step",
        description="Add 5 to the input value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    
    pipeline = Pipeline(id="ensemble_exception_pipeline", description="Test ensemble step exception")
    pipeline.ensemble([failing_step, add_step]).seal()
    
    with pytest.raises(RuntimeError, match="Step failed"):
        await pipeline.perform({"value": 10})

@pytest.mark.asyncio
async def test_cycle():
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    pipeline = Pipeline(id="cycle_pipeline", description="Test cycle")
    pipeline.cycle(
        gate=lambda data: data["value"] < 5,
        step=increment_step
    ).seal()
    
    result = await pipeline.perform({"value": 0})
    
    assert result["value"] == 5  # 0 -> 1 -> 2 -> 3 -> 4 -> 5

@pytest.mark.asyncio
async def test_cycle_with_max_cycles():
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    pipeline = Pipeline(id="max_cycles_pipeline", description="Test cycle with max cycles")
    pipeline.cycle(
        gate=lambda data: True,  # Always true gate
        step=increment_step,
        max_cycles=3
    ).seal()
    
    result = await pipeline.perform({"value": 0})
    
    assert result["value"] == 3  # 0 -> 1 -> 2 -> 3

@pytest.mark.asyncio
async def test_cycle_with_complex_gate():
    double_step = create_step(
        id="double",
        description="Double the value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )
    
    pipeline = Pipeline(id="complex_gate_pipeline", description="Test cycle with complex gate")
    pipeline.cycle(
        gate=lambda data: data["value"] < 100,
        step=double_step
    ).seal()
    
    result = await pipeline.perform({"value": 5})
    
    assert result["value"] == 160  # 5 -> 10 -> 20 -> 40 -> 80 -> 160

@pytest.mark.asyncio
async def test_cycle_never_executes():
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    pipeline = Pipeline(id="never_executes_pipeline", description="Test cycle that never executes")
    pipeline.cycle(
        gate=lambda data: data["value"] < 0,
        step=increment_step
    ).seal()
    
    result = await pipeline.perform({"value": 5})
    
    assert result["value"] == 5

@pytest.mark.asyncio
async def test_cycle_with_async_step():
    async def async_increment(params, perf):
        await asyncio.sleep(0.01)
        return {"value": params["payload"]["value"] + 1}
    
    increment_step = create_step(
        id="async_increment",
        description="Async increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=async_increment
    )
    
    pipeline = Pipeline(id="async_cycle_pipeline", description="Test cycle with async step")
    pipeline.cycle(
        gate=lambda data: data["value"] < 3,
        step=increment_step
    ).seal()
    
    result = await pipeline.perform({"value": 0})
    
    assert result["value"] == 3  # 0 -> 1 -> 2 -> 3

@pytest.mark.asyncio
async def test_cycle_with_error():
    def failing_handler(params, perf):
        if params["payload"]["value"] >= 3:
            raise RuntimeError("Step failed after 3 iterations")
        return {"value": params["payload"]["value"] + 1}
    
    increment_step = create_step(
        id="increment",
        description="Increment until failure",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=failing_handler
    )
    
    pipeline = Pipeline(id="cycle_error_pipeline", description="Test cycle with error")
    pipeline.cycle(
        gate=lambda data: data["value"] < 5,
        step=increment_step
    ).seal()
    
    with pytest.raises(RuntimeError, match="Step failed after 3 iterations"):
        await pipeline.perform({"value": 0})

@pytest.mark.asyncio
async def test_cycle_gate_exception():
    def failing_gate(data):
        if data["value"] >= 2:
            raise RuntimeError("Gate failed")
        return data["value"] < 5
    
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    pipeline = Pipeline(id="gate_error_pipeline", description="Test cycle with gate error")
    pipeline.cycle(
        gate=failing_gate,
        step=increment_step
    ).seal()
    
    with pytest.raises(RuntimeError, match="Gate failed"):
        await pipeline.perform({"value": 0})

@pytest.mark.asyncio
async def test_cycle_with_async_gate():
    async def async_gate(data):
        await asyncio.sleep(0.01)
        return data["value"] < 5
    
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    pipeline = Pipeline(id="async_gate_pipeline", description="Test cycle with async gate")
    with pytest.raises(ValueError, match="Cycle gates cannot be async functions"):
        pipeline.cycle(
            gate=async_gate,
            step=increment_step
        ).seal()

def test_cycle_with_none_step():
    pipeline = Pipeline(id="none_step_pipeline", description="Test cycle with None step")
    with pytest.raises(ValueError, match="Step cannot be None"):
        pipeline.cycle(
            gate=lambda data: data["value"] < 5,
            step=None
        ).seal()

@pytest.mark.asyncio
async def test_serial_then_cycle():
    add_five_step = create_step(
        id="add_five",
        description="Add 5 to the value",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 5}
    )
    
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    pipeline = Pipeline(id="serial_cycle_pipeline", description="Test serial then cycle")
    pipeline.pipe(add_five_step).cycle(
        gate=lambda data: data["value"] < 10,
        step=increment_step
    ).seal()
    
    result = await pipeline.perform({"value": 2})
    
    assert result["value"] == 10  # 2 -> 7 -> 8 -> 9 -> 10

@pytest.mark.asyncio
async def test_cycle_then_serial():
    increment_step = create_step(
        id="increment",
        description="Increment the counter",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] + 1}
    )
    
    multiply_by_two_step = create_step(
        id="multiply_by_two",
        description="Multiply by 2",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        handler=lambda params, perf: {"value": params["payload"]["value"] * 2}
    )
    
    pipeline = Pipeline(id="cycle_serial_pipeline", description="Test cycle then serial")
    pipeline.cycle(
        gate=lambda data: data["value"] < 5,
        step=increment_step
    ).pipe(multiply_by_two_step).seal()
    
    result = await pipeline.perform({"value": 2})
    
    assert result["value"] == 10  # 2 -> 3 -> 4 -> 5, then 5 * 2 = 10
