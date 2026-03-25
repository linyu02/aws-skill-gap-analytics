# Skill Gap Analytics (AWS + LLM)

Turn messy job postings into clear learning priorities.

![Demo](. /data analysis/weekly_skill_demand.gif)

---

## Why I Built This

Job descriptions made me feel like I needed to learn *everything*.

So I reframed the problem:

> Instead of asking “what should I learn?”,
> I asked **“what actually matters most?”**

👉 [Project story](YOUR_NOTION_LINK)

---

## Key Takeaways

![Top Skills](./assets/top_skills.png)

* Python and SQL dominate consistently
* Machine Learning is necessary but not sufficient
* Cloud (AWS) appears frequently but is underrepresented in my profile

![Coverage](./assets/coverage.png)

* ~65% of required skills matched
* ~35% identified as high-impact gaps

---



## How It Works

S3 → Lambda → Step Functions → DynamoDB → S3

* Extract skills using LLM (Amazon Bedrock)
* Aggregate demand across jobs
* Compare with resume
* Output gaps and next steps

![Architecture](./assets/architecture.png)

👉 [System design](YOUR_NOTION_LINK)

---

## Repo Guide

* `lambda/`
  Core AWS Lambda functions:

  * skill extraction (LLM)
  * aggregation (weekly counts)
  * resume comparison (match vs gap)
  * recommendation generation

* `step_functions/`
  Workflow definition for orchestrating the pipeline

* `scripts/`
  Local analysis and visualization (charts, GIFs)

* `assets/`
  Visualizations and architecture diagram used above

* `sample_output/`
  Example outputs (matched skills, gaps, recommendations)

---

## Stack

Python · AWS (S3, Lambda, Step Functions, DynamoDB) · Bedrock · Pandas
