from typing import Any, Callable, Dict, List, Union
from typing_extensions import TypedDict

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from symphony.performance import Performance

# Type aliases
InputData = Dict[str, Any]
OutputData = Dict[str, Any]
GateFunction = Callable[[InputData], bool]

# Step handler function signature with performance context
StepHandler = Callable[[Dict[str, InputData], 'Performance'], OutputData]

# TypedDict definitions for block structures
class SerialBlock(TypedDict):
    type: str
    step: Any

class EnsembleBlock(TypedDict):
    type: str
    steps: List[Any]

class ForkRoute(TypedDict):
    gate: GateFunction
    step: Any

class ForkBlock(TypedDict):
    type: str
    routes: List[ForkRoute]

class CycleBlock(TypedDict):
    type: str
    gate: GateFunction
    step: Any
    max_cycles: int

# Union type for all block types
Block = Union[SerialBlock, EnsembleBlock, ForkBlock, CycleBlock]
Score = List[Block]
