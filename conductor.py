import inspect
import asyncio
import logging
from enum import Enum
from typing import Any, Dict, List
from datetime import datetime

from symphony.types import (
    Score, 
    Block, 
    InputData, 
    OutputData,
    SerialBlock,
    EnsembleBlock,
    ForkBlock,
    CycleBlock
)
from symphony.performance import Performance

logger = logging.getLogger(__name__)


class BlockType(Enum):
    """Enumeration of supported execution block types."""
    SERIAL = "serial"
    ENSEMBLE = "ensemble"
    FORK = "fork"
    CYCLE = "cycle"


class Conductor:
    """
    Core execution engine for Symphony pipelines.
    
    Orchestrates the execution of different block types including serial steps,
    ensemble execution, conditional forking, and cycles.
    """
    
    @staticmethod
    async def run(
        score: Score, 
        input_data: InputData,
        pipeline_id: str,
        pipeline_metadata: Dict[str, Any] = None
    ) -> OutputData:
        """
        Execute a complete pipeline score.
        
        Args:
            score: List of execution blocks to process
            input_data: Initial input data
            pipeline_id: Unique identifier for the pipeline execution
            pipeline_metadata: Optional metadata for the pipeline
            
        Returns:
            Final output data after all blocks are executed
        """
        perf = Performance(
            pipeline_id=pipeline_id,
            pipeline_metadata=pipeline_metadata or {}
        )
        
        data: OutputData = input_data
        
        for block in score:
            data = await Conductor._process_block(block, data, perf)
        
        return data
    
    @staticmethod
    async def _process_block(
        block: Block, 
        data: InputData, 
        perf: Performance
    ) -> OutputData:
        """
        Route execution to the appropriate block type handler.
        
        Args:
            block: The execution block to process
            data: Input data for the block
            perf: Performance context
            
        Returns:
            Output data from the block execution
            
        Raises:
            ValueError: If block type is unknown or unhandled
        """
        try:
            block_type = BlockType(block["type"])
        except ValueError:
            raise ValueError(f"Unknown block type: {block['type']}")
        
        handlers = {
            BlockType.SERIAL: Conductor._process_serial,
            BlockType.ENSEMBLE: Conductor._process_ensemble,
            BlockType.FORK: Conductor._process_fork,
            BlockType.CYCLE: Conductor._process_cycle,
        }
        
        handler = handlers.get(block_type)
        if not handler:
            raise ValueError(f"Unhandled block type: {block_type}")
            
        return await handler(block, data, perf)
    
    @staticmethod
    async def _run_step(step: Any, data: InputData, perf: Performance) -> OutputData:
        """
        Execute a single step, handling both sync and async handler functions.
        
        Args:
            step: The step to execute
            data: Input data for the step
            perf: Performance context
            
        Returns:
            Output data from the step execution
        """
        params: Dict[str, InputData] = {"payload": data}
        
        # Update performance with current step info
        perf.step_id = step.id
        perf.step_started_at = datetime.utcnow()
        perf.step_number += 1
        
        # Execute the step
        if inspect.iscoroutinefunction(step.handler):
            result = await step.handler(params, perf)
        else:
            result = step.handler(params, perf)
        
        # Store the step result in performance for future steps to access
        perf.record_output(step.id, result)
        
        return result
    
    @staticmethod
    async def _process_serial(
        block: SerialBlock, 
        data: InputData, 
        perf: Performance
    ) -> OutputData:
        """
        Execute a single step serially.
        
        Args:
            block: Serial execution block
            data: Input data
            perf: Performance context
            
        Returns:
            Step execution result
        """
        step = block["step"]
        return await Conductor._run_step(step, data, perf)
    
    @staticmethod
    async def _process_ensemble(
        block: EnsembleBlock,
        data: InputData,
        perf: Performance
    ) -> OutputData:
        """
        Execute multiple steps in an ensemble (parallel).
        
        Args:
            block: Ensemble execution block
            data: Input data (shared by all steps)
            perf: Performance context
            
        Returns:
            Dictionary mapping step IDs to their results
        """
        steps = block["steps"]
        
        # Create step execution coroutines
        async def run_single_step(step):
            return await Conductor._run_step(step, data, perf)
        
        coroutines = [run_single_step(step) for step in steps]
        
        # Execute all steps in parallel
        results: List[OutputData] = await asyncio.gather(*coroutines)
        
        # Organize results by step ID
        ensemble_results = {step.id: result for step, result in zip(steps, results)}
        
        # Store combined ensemble results as a special entry
        perf.record_output("_ensemble_results", ensemble_results)
        
        return ensemble_results
    
    @staticmethod
    async def _process_fork(
        block: ForkBlock, 
        data: InputData, 
        perf: Performance
    ) -> OutputData:
        """
        Execute conditional forking - run the first step whose gate matches.
        
        Args:
            block: Fork execution block
            data: Input data
            perf: Performance context
            
        Returns:
            Result from the executed route, or input data if no gates match
        """
        routes = block["routes"]
        
        for route in routes:
            gate = route["gate"]
            
            if gate(data):
                step = route["step"]
                return await Conductor._run_step(step, data, perf)
        
        # If no gate matched, return data unchanged
        return data
    
    @staticmethod
    async def _process_cycle(
        block: CycleBlock, 
        data: InputData, 
        perf: Performance
    ) -> OutputData:
        """
        Execute a step repeatedly while gate condition is true.
        
        Args:
            block: Cycle execution block
            data: Initial input data
            perf: Performance context
            
        Returns:
            Final data after cycle completion
        """
        gate = block["gate"]
        step = block["step"]
        max_cycles: int = block.get("max_cycles", 100)
        
        cycle_count: int = 0
        current_data: OutputData = data
        
        while cycle_count < max_cycles:
            if not gate(current_data):
                break
            
            current_data = await Conductor._run_step(step, current_data, perf)
            cycle_count += 1
        
        if cycle_count >= max_cycles:
            logger.warning(f"Cycle reached maximum iterations ({max_cycles}) for pipeline {perf.pipeline_id}")
            
        return current_data
