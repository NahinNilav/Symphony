from typing import Any, Dict, Optional, List
from datetime import datetime
import uuid

from symphony.types import OutputData


class Performance:
    """
    Runtime context passed to every step containing metadata and execution state.
    
    The performance context provides access to pipeline metadata, execution timing,
    step outputs, and execution history. It enables steps to access data from
    previous stages and maintain state throughout the pipeline execution.
    """
    
    def __init__(
        self,
        pipeline_id: str,
        performance_id: Optional[str] = None,
        step_id: Optional[str] = None,
        step_number: int = 0,
        attempt_number: int = 1,
        pipeline_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize a performance context.
        
        Args:
            pipeline_id: Unique identifier of the executing pipeline
            performance_id: Unique identifier for this performance instance
            step_id: Current step identifier
            step_number: Current step number in the execution
            attempt_number: Attempt number for retry scenarios
            pipeline_metadata: Metadata associated with the pipeline
        """
        self.pipeline_id = pipeline_id
        self.performance_id = performance_id or f"perf_{uuid.uuid4().hex[:8]}"
        self.step_id = step_id
        self.step_number = step_number
        self.attempt_number = attempt_number
        self.pipeline_metadata = pipeline_metadata or {}
        
        # Timing information
        self.started_at = datetime.utcnow()
        self.step_started_at = datetime.utcnow()
        
        # Step outputs history
        self._step_outputs: Dict[str, OutputData] = {}
        self._history: List[Dict[str, Any]] = []
    
    def record_output(self, step_id: str, output: OutputData) -> None:
        """
        Record the output of a completed step.
        
        Args:
            step_id: Identifier of the completed step
            output: Output data from the step
        """
        self._step_outputs[step_id] = output
        
        entry = {
            "step_number": self.step_number,
            "step_id": step_id,
            "output": output,
            "timestamp": datetime.utcnow().isoformat(),
            "attempt_number": self.attempt_number
        }
        self._history.append(entry)
    
    def get_output(self, step_id: str) -> Optional[OutputData]:
        """
        Get the output from a previously executed step.
        
        Args:
            step_id: Identifier of the step whose output to retrieve
            
        Returns:
            Step output data, or None if step hasn't executed
        """
        return self._step_outputs.get(step_id)
    
    def get_all_outputs(self) -> Dict[str, OutputData]:
        """
        Get all step outputs from this performance.
        
        Returns:
            Dictionary mapping step IDs to their output data
        """
        return self._step_outputs.copy()
    
    def get_history(self) -> List[Dict[str, Any]]:
        """
        Get the complete execution history.
        
        Returns:
            List of execution records with timestamps and outputs
        """
        return self._history.copy()
    
    def create_child(
        self, 
        step_id: str, 
        step_number: Optional[int] = None,
        attempt_number: int = 1
    ) -> 'Performance':
        """
        Create a new performance context for a child step execution.
        
        Inherits the current context state while updating step-specific fields.
        
        Args:
            step_id: Identifier for the child step
            step_number: Step number for the child execution
            attempt_number: Attempt number for retry scenarios
            
        Returns:
            New Performance instance for the child step
        """
        child = Performance(
            pipeline_id=self.pipeline_id,
            performance_id=self.performance_id,
            step_id=step_id,
            step_number=step_number or (self.step_number + 1),
            attempt_number=attempt_number,
            pipeline_metadata=self.pipeline_metadata
        )
        
        # Copy step outputs and history to child
        child._step_outputs = self._step_outputs.copy()
        child._history = self._history.copy()
        child.started_at = self.started_at
        
        return child
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert performance context to dictionary for serialization.
        
        Returns:
            Dictionary representation of the performance context
        """
        return {
            "pipeline_id": self.pipeline_id,
            "performance_id": self.performance_id,
            "step_id": self.step_id,
            "step_number": self.step_number,
            "attempt_number": self.attempt_number,
            "pipeline_metadata": self.pipeline_metadata,
            "started_at": self.started_at.isoformat(),
            "step_started_at": self.step_started_at.isoformat(),
            "step_outputs": self._step_outputs,
            "history": self._history
        }
    
    def __repr__(self) -> str:
        """String representation of the performance context."""
        return (
            f"Performance(pipeline_id='{self.pipeline_id}', "
            f"performance_id='{self.performance_id}', "
            f"step_id='{self.step_id}', "
            f"step={self.step_number}, "
            f"attempt={self.attempt_number})"
        )
