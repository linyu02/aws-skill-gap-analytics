import os
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from botocore.config import Config

CFG = Config(
    connect_timeout=5,
    read_timeout=60,
    retries={"max_attempts": 2, "mode": "standard"}
)

s3 = boto3.client("s3", config=CFG)
dynamodb = boto3.resource("dynamodb", config=CFG)
brt = boto3.client("bedrock-runtime", config=CFG)

def normalize_skill(skill: Any) -> str:
    return str(skill or "").strip().lower()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_ddb_safe(v: Any) -> Any:
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, dict):
        return {k: _to_ddb_safe(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_to_ddb_safe(x) for x in v]
    return v


def _get_table_keys(table):
    pk = None
    sk = None
    for k in table.key_schema:
        if k["KeyType"] == "HASH":
            pk = k["AttributeName"]
        elif k["KeyType"] == "RANGE":
            sk = k["AttributeName"]
    if not pk:
        raise RuntimeError(f"Could not determine partition key for table {table.name}")
    return pk, sk


def _clean_skill_item(item: dict, *, require_evidence: bool) -> dict | None:
    if not isinstance(item, dict):
        return None

    skill = normalize_skill(item.get("skill"))
    if not skill:
        return None

    cleaned = {
        "skill": skill,
        "job_count": int(item.get("job_count", 0) or 0),
        "category": str(item.get("category") or "").strip().lower() or None,
        "match": bool(item.get("match"))
    }

    if require_evidence:
        evidence = str(item.get("evidence") or "").strip()
        cleaned["evidence"] = evidence

    return cleaned


def _get_reviewed_payload(event: dict) -> dict:
    """
    Prefer final human-reviewed output, then older shapes.
    """
    human_review = event.get("humanReview")
    if isinstance(human_review, dict):
        # Current callback result shape
        if "matched_skills" in human_review or "gap_skills" in human_review:
            return human_review

        # Older wrapped shape
        payload = human_review.get("Payload")
        if isinstance(payload, dict):
            return payload

    if isinstance(event.get("reviewed_output"), dict):
        return event["reviewed_output"]

    if isinstance(event.get("reviewed_payload"), dict):
        return event["reviewed_payload"]

    lambda_c = event.get("lambdaC")
    if isinstance(lambda_c, dict):
        payload = lambda_c.get("Payload")
        if isinstance(payload, dict):
            return payload

    for k in ("lambda_c_output", "lambdaC_payload"):
        payload = event.get(k)
        if isinstance(payload, dict):
            return payload

    return {}


def safe_json_from_model_text(text_out: str) -> dict:
    start = text_out.find("{")
    end = text_out.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise RuntimeError("Model did not return valid JSON")
    return json.loads(text_out[start:end + 1])


def make_recommendations(*, gaps: list[dict], max_recs: int = 5) -> list[dict]:
    gaps = [g for g in gaps if isinstance(g, dict) and normalize_skill(g.get("skill"))]
    gaps = gaps[:max_recs]

    if not gaps:
        return []

    model_id = os.environ["MODEL_ID"]
    system = "You are a concise data science career coach. Return JSON only. No commentary."

    prompt = f"""
You will be given a list of missing skills for a data science role.

Return JSON ONLY in this schema:
{{
  "recommendations": [
    {{
      "skill": "skill",
      "explanation": "1 sentence explaining why this matters for data science roles",
      "first_step": "1 concrete first step that can be done in under 2 hours"
    }}
  ]
}}

RULES:
- Keep explanation to 1 sentence.
- first_step must be specific and actionable.
- Return at most {len(gaps)} items.
- JSON only.

MISSING_SKILLS:
{json.dumps(gaps, ensure_ascii=False)}
""".strip()

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 500,
        "temperature": 0.2,
        "system": system,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
    }

    resp = brt.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body).encode("utf-8"),
    )

    raw = resp["body"].read().decode("utf-8", errors="replace")
    payload = json.loads(raw)

    content = payload.get("content") or []
    if not content or "text" not in content[0]:
        raise RuntimeError("Model response missing content text")

    text_out = content[0]["text"]
    parsed = safe_json_from_model_text(text_out)

    out = []
    seen = set()

    for it in (parsed.get("recommendations") or []):
        if not isinstance(it, dict):
            continue

        skill = normalize_skill(it.get("skill"))
        explanation = str(it.get("explanation") or "").strip()
        first_step = str(it.get("first_step") or "").strip()

        if not skill or not explanation or not first_step:
            continue
        if skill in seen:
            continue

        out.append({
            "skill": skill,
            "explanation": explanation,
            "first_step": first_step
        })
        seen.add(skill)

    return out[:max_recs]


def lambda_handler(event, context):
    week = str(event.get("week") or "").strip()
    if not week:
        raise ValueError("Missing required field: 'week'")

    reviewed_payload = _get_reviewed_payload(event)
    if not reviewed_payload:
        raise ValueError("Could not find reviewed payload in event")

    raw_matched_skills = reviewed_payload.get("matched_skills", []) or []
    raw_gap_skills = reviewed_payload.get("gap_skills", []) or []

    matched_skills = []
    for item in raw_matched_skills:
        cleaned = _clean_skill_item(item, require_evidence=True)
        if cleaned:
            matched_skills.append(cleaned)

    gap_skills = []
    for item in raw_gap_skills:
        cleaned = _clean_skill_item(item, require_evidence=False)
        if cleaned:
            gap_skills.append(cleaned)

    jobs_processed = (
        event.get("jobs_processed")
        or event.get("lambdaA", {}).get("Payload", {}).get("jobs_processed")
        or reviewed_payload.get("jobs_processed")
        or 0
    )

    run_utc = reviewed_payload.get("run_utc")
    resume_key = (
        reviewed_payload.get("resume_key")
        or event.get("resume_s3_key")
        or event.get("resume_key")
    )

    review_id = reviewed_payload.get("review_id")
    review_status = reviewed_payload.get("review_status")
    approved = reviewed_payload.get("approved")
    generated_at = now_utc_iso()

    recommendations = make_recommendations(
        gaps=gap_skills,
        max_recs=min(5, len(gap_skills))
    )
    recommendation_by_skill = {
        normalize_skill(r.get("skill")): r for r in recommendations if isinstance(r, dict)
    }

    bucket = os.environ["RESULTS_BUCKET"].strip()
    prefix = os.environ.get("RESULTS_PREFIX", "results/").strip().rstrip("/") + "/"
    filename = os.environ.get("RESULTS_FILENAME", "final-reviewed-results.json").strip()
    key = f"{prefix}{week}/{filename}".replace(" ", "")
    s3_uri = f"s3://{bucket}/{key}"

    final_body = {
        "week": week,
        "generated_at": generated_at,
        "jobs_processed": int(jobs_processed or 0),
        "review_status": review_status,
        "approved": approved,
        "review_id": review_id,
        "run_utc": run_utc,
        "resume_key": resume_key,
        "top_skills": reviewed_payload.get("top_skills", []),
        "matched_skills": matched_skills,
        "gap_skills": gap_skills,
        "overlap_count": reviewed_payload.get("overlap_count", len(matched_skills)),
        "gap_count": reviewed_payload.get("gap_count", len(gap_skills)),
        "recommendations": recommendations
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(final_body, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    gap_table_name = os.environ["DDB_GAP_TABLE"].strip()
    overlap_table_name = os.environ["DDB_OVERLAP_TABLE"].strip()

    gap_table = dynamodb.Table(gap_table_name)
    overlap_table = dynamodb.Table(overlap_table_name)

    gap_pk, gap_sk = _get_table_keys(gap_table)
    ov_pk, ov_sk = _get_table_keys(overlap_table)

    gap_overwrite_keys = [gap_pk] + ([gap_sk] if gap_sk else [])
    ov_overwrite_keys = [ov_pk] + ([ov_sk] if ov_sk else [])

    gap_items_written = 0
    overlap_items_written = 0

    with gap_table.batch_writer(overwrite_by_pkeys=gap_overwrite_keys) as batch:
        for g in gap_skills:
            skill = g["skill"]
            rec = recommendation_by_skill.get(skill)

            item = {
                gap_pk: week,
                **({gap_sk: skill} if gap_sk else {}),
                "skill": skill,
                "job_count": g.get("job_count", 0),
                "category": g.get("category"),
                "match": False,
                "run_utc": run_utc,
                "resume_key": resume_key,
                "generated_at": generated_at,
                "s3_uri": s3_uri,
                "review_id": review_id,
                "review_status": review_status,
                "approved": approved,
                "recommendation": rec,
            }

            batch.put_item(Item=_to_ddb_safe(item))
            gap_items_written += 1

    with overlap_table.batch_writer(overwrite_by_pkeys=ov_overwrite_keys) as batch:
        for o in matched_skills:
            skill = o["skill"]

            item = {
                ov_pk: week,
                **({ov_sk: skill} if ov_sk else {}),
                "skill": skill,
                "job_count": o.get("job_count", 0),
                "category": o.get("category"),
                "evidence": o.get("evidence"),
                "match": True,
                "run_utc": run_utc,
                "resume_key": resume_key,
                "generated_at": generated_at,
                "s3_uri": s3_uri,
                "review_id": review_id,
                "review_status": review_status,
                "approved": approved,
            }

            batch.put_item(Item=_to_ddb_safe(item))
            overlap_items_written += 1

    return {
        "status": "ok",
        "week": week,
        "review_id": review_id,
        "review_status": review_status,
        "approved": approved,
        "s3_bucket": bucket,
        "s3_key": key,
        "s3_uri": s3_uri,
        "recommendation_count": len(recommendations),
        "ddb_gap_table": gap_table_name,
        "ddb_overlap_table": overlap_table_name,
        "gap_items_written": gap_items_written,
        "overlap_items_written": overlap_items_written,
        "gap_pk_sk": {"pk": gap_pk, "sk": gap_sk},
        "overlap_pk_sk": {"pk": ov_pk, "sk": ov_sk},
    }

