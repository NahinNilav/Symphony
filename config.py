"""Configuration constants for the Symphony framework."""

class Config:
    """
    Global configuration settings for the Symphony framework.
    
    Contains default values for execution parameters like cycle iterations
    and timeout settings that can be referenced throughout the framework.
    """
    
    # Cycle settings
    DEFAULT_MAX_CYCLES: int = 100
    
    # Execution settings
    DEFAULT_TIMEOUT_SECONDS: int = 300
