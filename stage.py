from typing import List, Dict, Any, Optional, Type
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime

from symphony.pipeline import Pipeline


class RunPipelineRequest(BaseModel):
    """Request model for pipeline execution."""
    input_data: Dict[str, Any]

class RunPipelineResponse(BaseModel):
    """Response model for pipeline execution results."""
    pipeline_id: str
    status: str
    result: Dict[str, Any]
    duration_ms: float
    timestamp: datetime

class StepInfo(BaseModel):
    """Information about a step within a pipeline."""
    id: str
    description: str
    type: str  # "serial", "ensemble", "fork", "cycle"
    input_schema: Optional[Dict[str, str]] = None
    output_schema: Optional[Dict[str, str]] = None

class PipelineSummary(BaseModel):
    """Summary information about a pipeline."""
    id: str
    description: str
    steps: List[StepInfo]

class PipelineDetail(BaseModel):
    """Detailed information about a pipeline."""
    id: str
    description: str
    metadata: Dict[str, Any]
    steps: List[StepInfo]

class PipelineListResponse(BaseModel):
    """Response model for listing pipelines."""
    pipelines: List[PipelineSummary]

class Stage:
    """
    FastAPI server for hosting Symphony pipelines.
    
    Provides REST endpoints for discovering and executing pipelines.
    
    Example:
        pipelines = [pipeline1, pipeline2, pipeline3]
        app = Stage(pipelines=pipelines).get_app()
        
        if __name__ == "__main__":
            import uvicorn
            uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    """
    
    def __init__(self, pipelines: List[Pipeline]) -> None:
        """
        Initialize Stage with a list of pipelines.
        
        Args:
            pipelines: List of sealed Pipeline instances
            
        Raises:
            ValueError: If pipelines contain duplicates or unsealed pipelines
        """
        self.pipelines: Dict[str, Pipeline] = {}
        for pipeline in pipelines:
            if pipeline.id in self.pipelines:
                raise ValueError(f"Duplicate pipeline ID: {pipeline.id}")
            if not pipeline._sealed:
                raise ValueError(f"Pipeline {pipeline.id} must be sealed before adding to stage")
            self.pipelines[pipeline.id] = pipeline
    
    def _serialize_schema(self, schema_class: Type[BaseModel]) -> Optional[Dict[str, str]]:
        """
        Convert Pydantic model to a simple field:type mapping.
        
        Args:
            schema_class: Pydantic BaseModel class
            
        Returns:
            Dictionary mapping field names to simplified type strings, or None
        """
        if not schema_class:
            return None
        
        try:
            schema_dict = {}
            for field_name, field_info in schema_class.model_fields.items():
                field_type = field_info.annotation
                type_name = getattr(field_type, '__name__', str(field_type))
                
                if 'int' in type_name.lower():
                    schema_dict[field_name] = "int"
                elif 'float' in type_name.lower():
                    schema_dict[field_name] = "float"
                elif 'str' in type_name.lower():
                    schema_dict[field_name] = "string"
                elif 'bool' in type_name.lower():
                    schema_dict[field_name] = "boolean"
                elif 'list' in type_name.lower():
                    schema_dict[field_name] = "array"
                elif 'dict' in type_name.lower():
                    schema_dict[field_name] = "object"
                else:
                    schema_dict[field_name] = type_name
            
            return schema_dict
            
        except Exception:
            return {"error": "Could not parse schema"}
    
    def _extract_step_info(self, blocks: List[Dict[str, Any]]) -> List[StepInfo]:
        """
        Extract step information from execution blocks.
        
        Args:
            blocks: List of execution block dictionaries
            
        Returns:
            List of StepInfo objects
        """
        step_infos = []
        
        for block in blocks:
            block_type = block["type"]
            
            if block_type == "serial":
                step = block["step"]
                step_infos.append(StepInfo(
                    id=step.id,
                    description=step.description,
                    type="serial",
                    input_schema=self._serialize_schema(step.input_schema),
                    output_schema=self._serialize_schema(step.output_schema)
                ))
            
            elif block_type == "ensemble":
                for step in block["steps"]:
                    step_infos.append(StepInfo(
                        id=step.id,
                        description=step.description,
                        type="ensemble",
                        input_schema=self._serialize_schema(step.input_schema),
                        output_schema=self._serialize_schema(step.output_schema)
                    ))
            
            elif block_type == "fork":
                for route in block["routes"]:
                    step = route["step"]
                    step_infos.append(StepInfo(
                        id=step.id,
                        description=step.description,
                        type="fork",
                        input_schema=self._serialize_schema(step.input_schema),
                        output_schema=self._serialize_schema(step.output_schema)
                    ))
            
            elif block_type == "cycle":
                step = block["step"]
                step_infos.append(StepInfo(
                    id=step.id,
                    description=step.description,
                    type="cycle",
                    input_schema=self._serialize_schema(step.input_schema),
                    output_schema=self._serialize_schema(step.output_schema)
                ))
        
        return step_infos
    
    def get_app(self) -> FastAPI:
        """
        Create and configure the FastAPI application.
        
        Returns:
            Configured FastAPI application instance
        """
        app = FastAPI(
            title="Symphony Orchestration API",
            description="REST API for executing Symphony framework pipelines",
            version="1.0.0"
        )
        
        from fastapi.middleware.cors import CORSMiddleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        @app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {
                "status": "healthy",
                "pipelines_count": len(self.pipelines),
                "timestamp": datetime.utcnow().isoformat()
            }
        
        @app.get("/pipelines", response_model=PipelineListResponse)
        async def list_pipelines():
            """Get list of all available pipelines."""
            summaries = []
            for pipeline in self.pipelines.values():
                step_infos = self._extract_step_info(pipeline._steps)
                summaries.append(PipelineSummary(
                    id=pipeline.id,
                    description=pipeline.description,
                    steps=step_infos,
                ))
            
            return PipelineListResponse(pipelines=summaries)
        
        @app.get("/pipelines/{pipeline_id}", response_model=PipelineDetail)
        async def get_pipeline_details(pipeline_id: str):
            """Get detailed information about a specific pipeline."""
            if pipeline_id not in self.pipelines:
                raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
            
            pipeline = self.pipelines[pipeline_id]
            step_infos = self._extract_step_info(pipeline._steps)
            
            return PipelineDetail(
                id=pipeline.id,
                description=pipeline.description,
                metadata=pipeline.metadata,
                steps=step_infos,
            )
        
        @app.post("/pipelines/{pipeline_id}/perform", response_model=RunPipelineResponse)
        async def perform_pipeline(pipeline_id: str, request: RunPipelineRequest):
            """Execute a specific pipeline with input data."""
            if pipeline_id not in self.pipelines:
                raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
            
            pipeline = self.pipelines[pipeline_id]
            
            try:
                start_time = datetime.utcnow()
                result = await pipeline.perform(request.input_data)
                end_time = datetime.utcnow()
                
                duration_ms = round((end_time - start_time).total_seconds() * 1000, 4)
                
                return RunPipelineResponse(
                    pipeline_id=pipeline_id,
                    status="success",
                    result=result,
                    duration_ms=duration_ms,
                    timestamp=end_time
                )
                
            except Exception as e:
                raise HTTPException(
                    status_code=500, 
                    detail={
                        "error": str(e),
                        "pipeline_id": pipeline_id
                    }
                )
        
        return app
