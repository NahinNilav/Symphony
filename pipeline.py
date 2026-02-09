from typing import Any, List, Optional, Tuple, Dict
import inspect
import uuid

from symphony.conductor import Conductor, BlockType
from symphony.types import (
    InputData, 
    OutputData, 
    GateFunction,
    Block
)


class Pipeline:
    """
    A workflow orchestrator that allows building and executing complex data processing pipelines.
    
    Pipelines support serial execution, ensemble processing, conditional forking, and cycles.
    All pipelines must be sealed before execution.
    """
    
    def __init__(
        self, 
        id: Optional[str] = None, 
        description: Optional[str] = None
    ) -> None:
        """
        Initialize a new Pipeline.
        
        Args:
            id: Unique identifier for the pipeline. Auto-generated if not provided.
            description: Human-readable description of the pipeline's purpose.
        """
        self.id: str = id if id else f"pipeline_{uuid.uuid4().hex[:8]}"
        self.description: str = description if description else f"Pipeline {self.id}"
        self._steps: List[Block] = []
        self._sealed: bool = False
        self.metadata: Dict[str, Any] = {}
    
    def _check_sealed(self) -> None:
        """Ensure pipeline is not sealed when adding steps."""
        if self._sealed:
            raise RuntimeError("Cannot add steps after sealing")
    
    def _validate_step(self, step: Any) -> None:
        """Validate that a step is not None."""
        if step is None:
            raise ValueError("Step cannot be None")
    
    def _validate_gate(self, gate: GateFunction) -> None:
        """Validate that a gate function is not async."""
        if inspect.iscoroutinefunction(gate):
            raise ValueError("Fork gates cannot be async functions")
    
    def _validate_cycle_gate(self, gate: GateFunction) -> None:
        """Validate that a cycle gate function is not async."""
        if inspect.iscoroutinefunction(gate):
            raise ValueError("Cycle gates cannot be async functions")

    def annotate(self, key: str, value: Any) -> 'Pipeline':
        """
        Set metadata for this pipeline.
        
        Args:
            key: The metadata key
            value: The metadata value
            
        Returns:
            Self for method chaining
        """
        self.metadata[key] = value
        return self

    def pipe(self, step: Any) -> 'Pipeline':
        """
        Add a step to execute serially.
        
        Args:
            step: The step to execute
            
        Returns:
            Self for method chaining
            
        Raises:
            RuntimeError: If pipeline is already sealed
            ValueError: If step is None
        """
        self._check_sealed()
        self._validate_step(step)
        
        block: Block = {"type": BlockType.SERIAL.value, "step": step}
        self._steps.append(block)
        return self

    def ensemble(self, steps: List[Any]) -> 'Pipeline':
        """
        Add steps to execute as an ensemble (concurrently).
        
        Args:
            steps: List of steps to execute concurrently
            
        Returns:
            Self for method chaining
            
        Raises:
            RuntimeError: If pipeline is already sealed
            ValueError: If step list is empty or contains None values
        """
        self._check_sealed()
        if not steps:
            raise ValueError("Ensemble step list cannot be empty")
        
        for step in steps:
            self._validate_step(step)
        
        block: Block = {
            "type": BlockType.ENSEMBLE.value,
            "steps": list(steps)
        }
        self._steps.append(block)
        return self

    def fork(self, routes: List[Tuple[GateFunction, Any]]) -> 'Pipeline':
        """
        Add conditional forking logic.
        
        Executes the first step whose gate returns True.
        If no gates match, data passes through unchanged.
        
        Args:
            routes: List of (gate_function, step) tuples
            
        Returns:
            Self for method chaining
            
        Raises:
            RuntimeError: If pipeline is already sealed
            ValueError: If route list is empty, step is None, or gate is async
        """
        self._check_sealed()
        if not routes:
            raise ValueError("Fork list cannot be empty")
        
        for gate, step in routes:
            self._validate_step(step)
            self._validate_gate(gate)
        
        block: Block = {
            "type": BlockType.FORK.value,
            "routes": [{"gate": g, "step": s} for g, s in routes]
        }
        self._steps.append(block)
        return self

    def cycle(
        self, 
        gate: GateFunction, 
        step: Any,
        max_cycles: int = 100
    ) -> 'Pipeline':
        """
        Execute a step repeatedly while a gate condition is true.
        
        Args:
            gate: Function that returns True to continue cycling
            step: Step to execute on each iteration
            max_cycles: Maximum number of cycles to prevent infinite loops
            
        Returns:
            Self for method chaining
            
        Raises:
            RuntimeError: If pipeline is already sealed
            ValueError: If step is None or gate is async
        """
        self._check_sealed()
        self._validate_step(step)
        self._validate_cycle_gate(gate)
        
        block: Block = {
            "type": BlockType.CYCLE.value,
            "gate": gate,
            "step": step,
            "max_cycles": max_cycles
        }
        self._steps.append(block)
        return self

    def seal(self) -> 'Pipeline':
        """
        Seal the pipeline for execution.
        
        Must be called before performing the pipeline.
        Once sealed, no more steps can be added.
        
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If pipeline has no steps
        """
        if not self._steps:
            raise ValueError("Pipeline must have at least one step")
        self._sealed = True
        return self

    async def perform(self, input_data: InputData) -> OutputData:
        """
        Execute the pipeline with the provided input data.
        
        Args:
            input_data: Input data dictionary to process
            
        Returns:
            The final output data after all steps complete
            
        Raises:
            RuntimeError: If pipeline is not sealed
        """
        if not self._sealed:
            raise RuntimeError("Pipeline must be sealed before performing")
        
        return await Conductor.run(
            self._steps, 
            input_data, 
            pipeline_id=self.id,
            pipeline_metadata=self.metadata
        )
