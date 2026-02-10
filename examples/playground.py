from symphony import Stage
from symphony.examples.branched_flow import notification_pipeline
from loop_flow import retry_pipeline
from symphony.examples.parallel_flow import send_notification_pipeline
from sequential_flow import registration_pipeline

# Create stage with pipelines
app = Stage(pipelines=[notification_pipeline, retry_pipeline, send_notification_pipeline, registration_pipeline]).get_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("playground:app", host="0.0.0.0", port=8000, reload=True)
