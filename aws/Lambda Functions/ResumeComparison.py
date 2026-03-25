import os
import json
from datetime import datetime, timezone

import boto3
from botocore.config import Config

CFG = Config(
    connect_timeout=5,
    read_timeout=25,
    retries={"max_attempts": 2, "mode": "standard"}
)

s3 = boto3.client("s3", config=CFG)
brt = boto3.client(
    "bedrock-runtime",
    region_name=os.environ.get("BEDROCK_REGION"),
    config=CFG
)


def load_s3_text(bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="replace")


def normalize_skill(skill: str) -> str:
    return str(skill or "").strip().lower()


def chunk_list(items: list, size: int) -> list[list]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def parse_skill_lines(text_out: str, target_skills: list[str], resume_text: str) -> list[dict]:
    """
    Parse model output in the format:
      skill || evidence
      skill || evidence

    Returns:
      [{"skill": "...", "evidence": "..."}]
    """
    lower_targets = {normalize_skill(s) for s in target_skills}
    resume_text_lower = resume_text.lower()

    out = []
    seen = set()

    for raw_line in text_out.splitlines():
        line = raw_line.strip()

        if not line:
            continue

        # Ignore common wrappers if the model slips
        if line.startswith("```") or line.lower() in {"skills", "matches"}:
            continue

        if line.upper() == "NONE":
            continue

        if "||" not in line:
            continue

        skill_part, evidence_part = line.split("||", 1)
        skill = normalize_skill(skill_part)
        evidence = evidence_part.strip().strip('"').strip("'").strip()

        if not skill or not evidence:
            continue
        if skill not in lower_targets:
            continue
        if evidence.lower() not in resume_text_lower:
            continue
        if skill in seen:
            continue

        out.append({
            "skill": skill,
            "evidence": evidence
        })
        seen.add(skill)

    return out


def extract_resume_matches_chunk(
    resume_text: str,
    target_skills: list[str]
) -> list[dict]:
    """
    Ask the model to return only matched skills in line format:
      skill || evidence
    """
    model_id = os.environ["MODEL_ID"]
    system = "You extract information. Return only the requested line format. No commentary."

    prompt = f"""
You will be given a resume text and a list of target skills.

TASK:
Return only the target skills that are explicitly supported by the resume.

OUTPUT FORMAT:
One match per line in exactly this format:
skill || evidence

EXAMPLE:
python || Python (Pandas) and SQL
tableau || interactive Tableau dashboards

RULES:
- One match per line.
- Use exactly " || " as the delimiter.
- Only include skills from TARGET_SKILLS.
- Only include a skill if it is explicitly supported by the resume text.
- evidence must be a short verbatim substring from the resume, 2-8 words.
- Avoid quotation marks inside evidence.
- Do not use bullets or numbering.
- Do not return JSON.
- Do not return markdown.
- Do not add explanations.
- If no skills are supported, return exactly: NONE

TARGET_SKILLS:
{json.dumps(target_skills, ensure_ascii=False)}

RESUME:
{resume_text}
""".strip()

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 300,
        "temperature": 0.0,
        "system": system,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}]
            }
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
    text_out = payload["content"][0]["text"]

    print("MODEL_OUTPUT_CHUNK:", text_out)

    return parse_skill_lines(
        text_out=text_out,
        target_skills=target_skills,
        resume_text=resume_text
    )


def extract_resume_matches_with_evidence(
    resume_text: str,
    target_skills: list[str]
) -> list[dict]:
    """
    Batch target skills to reduce malformed / noisy output risk.
    Returns:
      [{"skill": "...", "evidence": "..."}]
    """
    all_matches = []
    seen = set()

    # Smaller batches = more stable output
    for chunk in chunk_list(target_skills, 5):
        try:
            chunk_matches = extract_resume_matches_chunk(
                resume_text=resume_text,
                target_skills=chunk
            )
        except Exception as e:
            print(f"Chunk failed for skills {chunk}: {e}")
            chunk_matches = []

        for item in chunk_matches:
            sk = item["skill"]
            if sk not in seen:
                all_matches.append(item)
                seen.add(sk)

    return all_matches


def lambda_handler(event, context):
    week = event.get("week")
    if not week:
        raise ValueError("Missing required field: 'week'")

    top_skills = event.get("top_skills")
    if not isinstance(top_skills, list) or not top_skills:
        raise ValueError("Missing required field: 'top_skills' (must be a non-empty list)")

    limit = int(event.get("limit", 10))
    top_skills = top_skills[:limit]

    bucket = os.environ["BUCKET_NAME"]
    resume_text = event.get("resume_text")
    resume_key = event.get("resume_s3_key")

    if not resume_text:
        if not resume_key:
            raise ValueError("Provide either 'resume_s3_key' or 'resume_text'")
        resume_text = load_s3_text(bucket, resume_key)

    run_utc = datetime.now(timezone.utc).isoformat()

    target_skill_names = [
        str(x.get("skill", "")).strip()
        for x in top_skills
        if x.get("skill")
    ]

    matches = extract_resume_matches_with_evidence(
        resume_text=resume_text,
        target_skills=target_skill_names
    )

    top_by_skill = {}
    for item in top_skills:
        sk = normalize_skill(item.get("skill"))
        if not sk:
            continue
        top_by_skill[sk] = {
            "skill": sk,
            "job_count": int(item.get("job_count", 0) or 0),
            "category": item.get("category")
        }

    matched_skills = []
    for match in matches:
        sk = normalize_skill(match.get("skill"))
        ev = (match.get("evidence") or "").strip()
        if not sk or not ev:
            continue

        meta = top_by_skill.get(sk, {"job_count": 0, "category": None})
        matched_skills.append({
            "skill": sk,
            "job_count": meta["job_count"],
            "category": meta["category"],
            "evidence": ev,
            "match": True
        })

    present = {item["skill"] for item in matched_skills}

    gap_skills = []
    for item in top_skills:
        sk = normalize_skill(item.get("skill"))
        if not sk or sk in present:
            continue

        gap_skills.append({
            "skill": sk,
            "job_count": int(item.get("job_count", 0) or 0),
            "category": item.get("category"),
            "match": False
        })

    gap_skills = sorted(
        gap_skills,
        key=lambda x: int(x.get("job_count", 0) or 0),
        reverse=True
    )

    return {
        "status": "pending_human_review",
        "week": week,
        "run_utc": run_utc,
        "resume_key": resume_key,
        "top_skills": top_skills,
        "matched_skills": matched_skills,
        "gap_skills": gap_skills,
        "overlap_count": len(matched_skills),
        "gap_count": len(gap_skills)
    }
