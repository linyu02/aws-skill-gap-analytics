import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config

CFG = Config(
    connect_timeout=5,
    read_timeout=10,
    retries={"max_attempts": 2, "mode": "standard"}
)

dynamodb = boto3.resource("dynamodb", config=CFG)
sns = boto3.client("sns", config=CFG)

table = dynamodb.Table(os.environ["REVIEW_TABLE"])
REVIEW_TOPIC_ARN = os.environ["REVIEW_TOPIC_ARN"]
REVIEW_APP_BASE_URL = os.environ.get("REVIEW_APP_BASE_URL", "http://localhost:5173").rstrip("/")


def lambda_handler(event, context):
    """
    Expected input from Step Functions:
    {
      "taskToken": "...",
      "reviewData": {
        "status": "pending_human_review",
        "week": "2026-W03",
        "run_utc": "2026-03-11T18:20:00Z",
        "resume_key": "resumes/current_resume.txt",
        "top_skills": [...],
        "matched_skills": [...],
        "gap_skills": [...],
        "overlap_count": 1,
        "gap_count": 2
      }
    }
    """
    task_token = event.get("taskToken")
    review_data = event.get("reviewData")

    if not task_token:
        raise ValueError("Missing required field: 'taskToken'")
    if not isinstance(review_data, dict):
        raise ValueError("Missing required field: 'reviewData' (must be an object)")

    review_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    week = str(review_data.get("week") or "").strip()
    review_url = f"{REVIEW_APP_BASE_URL}/review/{review_id}"

    item = {
        "review_id": review_id,
        "status": "PENDING",
        "task_token": task_token,
        "payload": review_data,
        "created_at": now,
        "updated_at": now
    }

    # 1. Save review record first
    table.put_item(Item=item)

    # 2. Send SNS email notification
    subject = "Human review needed for Skill Gap Analytics"
    message = (
        "A human review is ready.\n\n"
        f"week: {week or 'unknown'}\n"
        f"review_id: {review_id}\n\n"
        f"Open review:\n{review_url}\n"
    )

    sns.publish(
        TopicArn=REVIEW_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

    return {
        "review_id": review_id,
        "status": "PENDING",
        "message": "Human review created and notification sent"
    }
