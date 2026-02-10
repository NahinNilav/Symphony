"""
Forked Pipeline Example: Conditional Notification Pipeline

This example demonstrates a forked pipeline where notifications are sent
based on user preferences. Shows how .fork() executes only the first 
matching gate, even when multiple gates would match.
"""

from symphony import Pipeline, create_step
from pydantic import BaseModel
from typing import Dict, Any
import asyncio

# Data schemas
class UserPreferences(BaseModel):
    user_id: str
    email_enabled: bool
    sms_enabled: bool
    whatsapp_enabled: bool

class NotificationSent(BaseModel):
    user_id: str
    channel: str
    sent: bool

# Notification steps
def send_email(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Send email notification."""
    user = params["payload"]
    return {
        "user_id": user["user_id"],
        "channel": "email",
        "sent": True
    }

def send_sms(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Send SMS notification."""
    user = params["payload"]
    return {
        "user_id": user["user_id"],
        "channel": "sms", 
        "sent": True
    }

def send_whatsapp(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """Send WhatsApp notification."""
    user = params["payload"]
    return {
        "user_id": user["user_id"],
        "channel": "whatsapp",
        "sent": True
    }

# Create steps
email_step = create_step(
    id="email",
    description="Send email",
    input_schema=UserPreferences,
    output_schema=NotificationSent,
    handler=send_email
)

sms_step = create_step(
    id="sms",
    description="Send SMS",
    input_schema=UserPreferences,
    output_schema=NotificationSent,
    handler=send_sms
)

whatsapp_step = create_step(
    id="whatsapp",
    description="Send WhatsApp",
    input_schema=UserPreferences,
    output_schema=NotificationSent,
    handler=send_whatsapp
)

# Forked notification pipeline
notification_pipeline = Pipeline(id="conditional_notifications", description="Conditional notification pipeline")
notification_pipeline.fork([
    (lambda data: data.get("email_enabled", False), email_step),
    (lambda data: data.get("sms_enabled", False), sms_step),
    (lambda data: data.get("whatsapp_enabled", False), whatsapp_step)
]).seal()

async def main():
    """Run the forked notification pipeline example."""
    
    user = {
        "user_id": "user_001",
        "email_enabled": True,
        "sms_enabled": True,
        "whatsapp_enabled": False
    }
    
    print(f"User preferences: email={user['email_enabled']}, sms={user['sms_enabled']}, whatsapp={user['whatsapp_enabled']}")
    
    try:
        result = await notification_pipeline.perform(user)
        print(f"Notification sent via: {result['channel']}")
        print(f"First matching gate executed (email has priority)")
        
    except Exception as e:
        print(f"ERROR - {e}")

if __name__ == "__main__":
    asyncio.run(main())
