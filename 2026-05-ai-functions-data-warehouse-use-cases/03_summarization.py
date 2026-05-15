# Databricks notebook source
# MAGIC %md
# MAGIC # Use Case 3: Long-form Summarization for BI Workflows
# MAGIC
# MAGIC **What this notebook does:** Uses `ai_extract` (v2) for the structured fields and `ai_query` against `databricks-gpt-oss-120b` for the bespoke one-sentence summary
# MAGIC to extract typed fields from long-form text - turning unstructured call transcripts into a queryable BI table.
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - Databricks SQL warehouse (Serverless recommended) or DBR 14.3+
# MAGIC - Unity Catalog + AI Functions enabled
# MAGIC
# MAGIC **Estimated cost:** ~1–2 DBU per run (5 long transcripts, larger token count than short-text patterns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create dummy call transcripts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_call_transcripts AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (
# MAGIC     'CALL-001', 'ACCT-0042', '2026-04-14',
# MAGIC     'Rep: Thanks for joining today. So where are things with the renewal?
# MAGIC Customer: Honestly we are still evaluating. Our CFO asked us to look at two other options. We like the platform but the price jump at renewal caught us off guard.
# MAGIC Rep: What price delta are we talking about?
# MAGIC Customer: About 40%. We budgeted for 15. Our head of data engineering is actually a big fan and wants to stay but we need to get this down.
# MAGIC Rep: I can get you to commercial by end of week. Can we schedule a call Thursday with your CFO?
# MAGIC Customer: Yes, Thursday afternoon works. Let us say 2pm Pacific.
# MAGIC Rep: Done. I am going to flag this to my manager and see if we can move on multi-year pricing before that call.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-002', 'ACCT-0088', '2026-04-15',
# MAGIC     'Rep: How did the POC go?
# MAGIC Customer: Really well. We processed 2 million records in about 40 minutes. Our old system took overnight.
# MAGIC Rep: Great. Any blockers before we move to contract?
# MAGIC Customer: One - we need SSO sorted before we can sign. Our infosec team is not going to approve without it.
# MAGIC Rep: SSO is standard in the enterprise tier. I will send you the SAML docs today. How long does infosec usually take?
# MAGIC Customer: If we get them docs this week, probably two weeks review. So we are looking at signing early May.
# MAGIC Rep: Perfect. I will loop in our integration engineer to get you through onboarding fast once signed.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-003', 'ACCT-0115', '2026-04-16',
# MAGIC     'Rep: What is the update on the Snowflake migration project?
# MAGIC Customer: We have hit a wall. Our DBT models have hard-coded Snowflake syntax and the refactor is taking longer than expected. We are about 60% done.
# MAGIC Rep: Have you looked at the Databricks migration accelerator?
# MAGIC Customer: We tried it but it did not handle the window function syntax we use heavily. We are basically doing it manually.
# MAGIC Rep: I want to bring in our migration engineering team. They have done this exact pattern for three other customers this quarter. Can we get a technical sync?
# MAGIC Customer: Yes please. If you can get someone on by end of this week that would be incredibly helpful. We have a hard cutover deadline of June 1.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-004', 'ACCT-0204', '2026-04-17',
# MAGIC     'Rep: I wanted to check in - you mentioned last month you were exploring AI use cases.
# MAGIC Customer: Yes, we have been running a small experiment. We are using your AI Functions to classify customer complaint emails. It is working well but we are not sure how to scale it.
# MAGIC Rep: What does your current setup look like?
# MAGIC Customer: One data engineer, running it manually on a sample every Friday. We want to make it daily and connect it to our CRM.
# MAGIC Rep: That is exactly the kind of workflow our SQL warehouse is built for. Let me send you a notebook template.
# MAGIC Customer: That would be great. We are also wondering about cost - we are worried it will get expensive.
# MAGIC Rep: The AI Functions pricing is DBU-based, same model as everything else. I can run a cost estimate before your next planning cycle.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'CALL-005', 'ACCT-0331', '2026-04-18',
# MAGIC     'Rep: How is the platform performing after the first month live?
# MAGIC Customer: Honestly, better than expected. Query times are down significantly compared to what we had before. The team is happy.
# MAGIC Rep: Any concerns going into Q2?
# MAGIC Customer: Two things. One - we are going to need more compute headroom as we onboard three more business units in May. Two - we have a compliance audit in Q3 and need documentation on your data lineage capabilities.
# MAGIC Rep: For compute, I will send you the capacity planning guide and connect you with our platform team. For lineage, Unity Catalog has a full API-level lineage export. I will send you the compliance documentation today.
# MAGIC Customer: Perfect. Overall very happy with where things are. Looking forward to the next quarter.'
# MAGIC   )
# MAGIC AS t(call_id, account_id, call_date, transcript);
# MAGIC
# MAGIC SELECT call_id, account_id, call_date, LEFT(transcript, 100) AS transcript_preview FROM demo_call_transcripts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract structured fields with `ai_extract` + a bespoke summary via `ai_query`
# MAGIC
# MAGIC `ai_extract` v2 takes a JSON array of field names and returns a struct keyed by those names. For the
# MAGIC one-sentence "next step" - which doesn't fit a flat field - call `ai_query` against
# MAGIC `databricks-gpt-oss-120b` (the recommended general-purpose model).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   call_id,
# MAGIC   account_id,
# MAGIC   call_date,
# MAGIC   ai_extract(
# MAGIC     transcript,
# MAGIC     '["owner","deal_stage","risk_flag","risk_reason"]'
# MAGIC   ) AS facts,
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-120b',
# MAGIC     CONCAT(
# MAGIC       'In one sentence, what is the agreed next step from this sales call transcript? ',
# MAGIC       'Transcript: ', transcript
# MAGIC     )
# MAGIC   ) AS next_step
# MAGIC FROM demo_call_transcripts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Flatten for BI consumption

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH summarized AS (
# MAGIC   SELECT
# MAGIC     call_id,
# MAGIC     account_id,
# MAGIC     call_date,
# MAGIC     ai_extract(
# MAGIC       transcript,
# MAGIC       '["owner","deal_stage","risk_flag","risk_reason"]'
# MAGIC     ) AS facts,
# MAGIC     ai_query(
# MAGIC       'databricks-gpt-oss-120b',
# MAGIC       CONCAT(
# MAGIC         'In one sentence, what is the agreed next step from this sales call transcript? ',
# MAGIC         'Transcript: ', transcript
# MAGIC       )
# MAGIC     ) AS next_step
# MAGIC   FROM demo_call_transcripts
# MAGIC )
# MAGIC SELECT
# MAGIC   call_id,
# MAGIC   account_id,
# MAGIC   call_date,
# MAGIC   next_step,
# MAGIC   facts.owner,
# MAGIC   facts.deal_stage,
# MAGIC   facts.risk_flag,
# MAGIC   facts.risk_reason
# MAGIC FROM summarized
# MAGIC ORDER BY facts.risk_flag DESC, call_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | call_id | account_id | deal_stage | owner | risk_flag | risk_reason | next_step |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | CALL-001 | ACCT-0042 | negotiation | Rep | true | 40% price increase vs 15% budget; CFO evaluating competitors | Rep to align on multi-year pricing before Thursday CFO call |
# MAGIC | CALL-003 | ACCT-0115 | technical_validation | Rep | true | June 1 hard cutover deadline; migration 60% complete and blocked on window function syntax | Rep to bring in migration engineering team by end of week |
# MAGIC | CALL-002 | ACCT-0088 | negotiation | Rep | false | null | Rep to send SAML docs; infosec review expected ~2 weeks |
# MAGIC | CALL-004 | ACCT-0204 | evaluation | Rep | false | null | Rep to send AI Functions notebook template and cost estimate |
# MAGIC | CALL-005 | ACCT-0331 | renewal | Rep | false | null | Rep to send capacity planning guide and Unity Catalog compliance docs |
# MAGIC
# MAGIC ## Key behavior to verify
# MAGIC - `ai_extract` returns a struct - `facts.owner`, `facts.deal_stage`, etc. - keyed by the field names you pass in the JSON array. No `from_json` step needed (that was the v1 + `ai_query`/`responseFormat` pattern).
# MAGIC - The `owner` field typically returns `Rep` or `Customer` since the transcripts use role labels rather than personal names. If your real transcripts include speaker names, ai_extract will pull them out directly.
# MAGIC - `risk_flag` is inferred from the transcript: CALL-001 (price shock + competitive eval) and CALL-003 (hard deadline + blocked migration) are the expected risk rows.
# MAGIC - For richer label control (e.g. constraining `deal_stage` to a fixed enum), wrap the field in `ai_classify` with explicit labels instead of relying on free-text extraction.
# MAGIC
# MAGIC ## What to do next
# MAGIC - Point this at `gold.call_transcripts` and schedule it nightly
# MAGIC - `MERGE INTO gold.call_summaries` by `call_id` to keep the table current
# MAGIC - Connect a BI dashboard to filter `WHERE risk_flag = true` for a real-time deal risk view
# MAGIC - Version the prompt by keeping it in a `gold.prompt_registry` table - same change management as your dbt models