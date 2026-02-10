"""
Ensemble Pipeline Example: Send Notification Pipeline

This example demonstrates an ensemble pipeline where multiple welcome notifications
are sent simultaneously to a newly registered user. Shows how .ensemble() executes
independent steps concurrently for improved performance.
"""

from symphony import Pipeline, create_step
from pydantic import BaseModel
from typing import Dict, Any
import asyncio
import uuid

# Data schemas
class UserData(BaseModel):
    user_id: str
    email: str
    phone: str
    first_name: str

class NotificationResult(BaseModel):
    channel: str
    user_id: str
    message_id: str
    sent: bool
    delivery_time: float

class EnsembleResults(BaseModel):
    email: NotificationResult
    sms: NotificationResult
    whatsapp: NotificationResult

class Summary(BaseModel):
    user_id: str
    notifications_sent: int
    total_time: float
    success: bool

# Ensemble Step 1: Send Email
def send_email(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Send welcome email to the user."""
    user = params["payload"]
    
    return {
        "channel": "email",
        "user_id": user["user_id"],
        "message_id": f"email_{uuid.uuid4().hex[:8]}",
        "sent": True,
        "delivery_time": 1.2
    }

# Ensemble Step 2: Send SMS
def send_sms(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Send welcome SMS to the user."""
    user = params["payload"]
    
    return {
        "channel": "sms",
        "user_id": user["user_id"],
        "message_id": f"sms_{uuid.uuid4().hex[:8]}",
        "sent": True,
        "delivery_time": 0.8
    }

# Ensemble Step 3: Send WhatsApp
def send_whatsapp(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Send welcome WhatsApp message to the user."""
    user = params["payload"]
    
    return {
        "channel": "whatsapp",
        "user_id": user["user_id"],
        "message_id": f"whatsapp_{uuid.uuid4().hex[:8]}",
        "sent": True,
        "delivery_time": 1.5
    }

# Aggregation Step: Summarize Results
def summarize_results(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Aggregate results from all ensemble notification steps."""
    results = params["payload"]
    
    # Count successful notifications
    sent_count = sum(1 for result in results.values() if result["sent"])
    
    # Get maximum time (since they ran in ensemble)
    max_time = max(result["delivery_time"] for result in results.values())
    
    return {
        "user_id": list(results.values())[0]["user_id"],
        "notifications_sent": sent_count,
        "total_time": max_time,
        "success": sent_count == 3
    }

# Create steps
email_step = create_step(
    id="email",
    description="Send email notification",
    input_schema=UserData,
    output_schema=NotificationResult,
    handler=send_email
)

sms_step = create_step(
    id="sms", 
    description="Send SMS notification",
    input_schema=UserData,
    output_schema=NotificationResult,
    handler=send_sms
)

whatsapp_step = create_step(
    id="whatsapp",
    description="Send WhatsApp notification", 
    input_schema=UserData,
    output_schema=NotificationResult,
    handler=send_whatsapp
)

summary_step = create_step(
    id="summary",
    description="Summarize notification results",
    input_schema=EnsembleResults,
    output_schema=Summary,
    handler=summarize_results
)

# Ensemble send notification pipeline
send_notification_pipeline = Pipeline(id="send_notifications", description="Ensemble send notification pipeline")
send_notification_pipeline.ensemble([
    email_step,
    sms_step, 
    whatsapp_step
]).pipe(summary_step).seal()

async def main():
    """Run the send notification pipeline example."""
    
    user = {
        "user_id": "user_abc123",
        "email": "user@symphony.dev",
        "phone": "+1234567890",
        "first_name": "Alex"
    }
    
    try:
        result = await send_notification_pipeline.perform(user)
        print(result)
        print("pipeline completed successfully!")
    except Exception as e:
        print(f"ERROR - {e}")

if __name__ == "__main__":
    asyncio.run(main())
