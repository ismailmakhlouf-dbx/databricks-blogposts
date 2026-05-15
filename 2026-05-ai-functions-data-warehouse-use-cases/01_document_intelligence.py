# Databricks notebook source
# MAGIC %md
# MAGIC # Use Case 1: Document Intelligence - Turning PDFs into Rows
# MAGIC
# MAGIC **What this notebook does:** Demonstrates how to use `ai_parse_document` + `ai_extract` (v2) to extract structured
# MAGIC data from PDF documents stored in cloud storage - no OCR service, no Python pre-processing.
# MAGIC
# MAGIC **What you need to run this:**
# MAGIC - A Databricks SQL warehouse (Serverless recommended) or compute cluster with DBR 14.3+
# MAGIC - Unity Catalog enabled on your workspace
# MAGIC - AI Functions enabled (Settings → Workspace Admin → AI Functions)
# MAGIC - The dummy data below does not require real PDFs - it simulates parsed document content
# MAGIC
# MAGIC **Estimated cost:** < 0.5 DBU per run (5 rows, short documents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create dummy invoice data
# MAGIC
# MAGIC In production, `ai_parse_document` reads binary PDF content directly from cloud storage.
# MAGIC For this demo, we simulate the *output* of `ai_parse_document` - the extracted raw text -
# MAGIC so you can see how `ai_extract` processes it without needing real PDFs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop and recreate a demo catalog/schema (change these names to match your Unity Catalog setup)
# MAGIC -- CREATE CATALOG IF NOT EXISTS aifunctions_demo;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS aifunctions_demo.pattern1;
# MAGIC
# MAGIC -- For this demo we use a temp view so nothing is written to permanent storage
# MAGIC CREATE OR REPLACE TEMP VIEW demo_invoice_text AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (
# MAGIC     'INV-001',
# MAGIC     'INVOICE\nVendor: Acme Supplies Ltd\nInvoice No: INV-2026-0042\nDate: 2026-04-15\nBill To: Globex Corp\nItem: Industrial Widgets (x50)  $1,200.00\nItem: Shipping                   $45.00\nTotal Due: USD 1,245.00\nPayment Terms: Net 30'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-002',
# MAGIC     'RECHNUNG\nLieferant: Schmidt GmbH\nRechnungs-Nr: RG-2026-0099\nDatum: 15. April 2026\nAn: Mustermann AG\nPosition: Ersatzteile (x10)  EUR 880,00\nPosition: Versand             EUR 35,00\nGesamtbetrag: EUR 915,00\nZahlungsziel: 14 Tage netto'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-003',
# MAGIC     'TAX INVOICE\nSupplier: Pacific Tech Solutions\nInvoice #: PTS-00711\nDate: 16-Apr-2026\nCustomer: Blue Ocean Ltd\nDescription: Software License (Annual)  AUD 4,800.00\nGST (10%):                             AUD   480.00\nTotal Payable: AUD 5,280.00'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-004',
# MAGIC     'FACTURE\nFournisseur: Dupont Industrie\nNumero: F-2026-155\nDate: 17 avril 2026\nClient: Renard SARL\nProduit: Composants electroniques (x200)  1 600,00 EUR\nTransport:                                    80,00 EUR\nTotal TTC: 1 680,00 EUR'
# MAGIC   ),
# MAGIC   (
# MAGIC     'INV-005',
# MAGIC     'INVOICE\nFrom: Apex Consulting Group\nInvoice ID: ACG-2026-Q2-003\nIssue Date: April 18, 2026\nTo: Horizon Capital\nService: Q1 Strategy Advisory (40 hrs @ $350)  $14,000.00\nExpenses (travel):                              $1,240.00\nSubtotal: $15,240.00\nTax (8.5%): $1,295.40\nTotal: USD 16,535.40'
# MAGIC   )
# MAGIC AS t(document_id, parsed_text);
# MAGIC
# MAGIC SELECT * FROM demo_invoice_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b: How `ai_parse_document` feeds into `ai_extract` (production pattern)
# MAGIC
# MAGIC In production, you do not simulate the parsed text - you read real binary files and let
# MAGIC `ai_parse_document` convert them. Here is how the two functions chain in a single query:
# MAGIC
# MAGIC 1. **Inner SELECT**: `read_files(...)` with `format => 'binaryFile'` reads the raw bytes of each file.
# MAGIC    `ai_parse_document(content)` converts those bytes into a structured text string - preserving
# MAGIC    headers, tables, and layout from the original PDF or image. The result is aliased as `parsed_content`.
# MAGIC
# MAGIC 2. **Outer SELECT**: `ai_extract(...)` receives `parsed_content` and a JSON array of field names. It
# MAGIC    returns a struct keyed by those names. The model never sees raw binary - only the structured text
# MAGIC    that `ai_parse_document` extracted.
# MAGIC
# MAGIC The query planner handles both functions in one execution plan. No intermediate files, no Python glue.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Production pattern: ai_parse_document → ai_extract in a single query
# MAGIC -- Replace 's3://invoices/inbox/' with your actual object storage path.
# MAGIC -- This cell is commented out because it requires real binary PDFs to run.
# MAGIC -- Uncomment and replace the path to use with actual documents.
# MAGIC
# MAGIC /*
# MAGIC SELECT
# MAGIC   document_path,
# MAGIC   ai_extract(
# MAGIC     parsed_content,
# MAGIC     '["vendor_name","invoice_no","invoice_date","total_amount","currency","line_items"]'
# MAGIC   ) AS extracted
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     path AS document_path,
# MAGIC     ai_parse_document(content) AS parsed_content
# MAGIC   FROM read_files('s3://invoices/inbox/', format => 'binaryFile')
# MAGIC );
# MAGIC */
# MAGIC
# MAGIC -- Steps 2 and 3 below use simulated parsed_text to demonstrate ai_extract without real PDFs.
# MAGIC SELECT 'Replace the path above and uncomment to run on real documents.' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract structured fields with `ai_extract`
# MAGIC
# MAGIC `ai_extract` v2 takes a JSON array of field names and returns a struct keyed by those names. No prompt
# MAGIC engineering, no `responseFormat`, no `from_json`. This replaces the older `ai_query` + STRUCT pattern
# MAGIC for plain field extraction.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   document_id,
# MAGIC   ai_extract(
# MAGIC     parsed_text,
# MAGIC     '["vendor_name","invoice_number","invoice_date","total_amount","currency_code","payment_terms"]'
# MAGIC   ) AS extracted
# MAGIC FROM demo_invoice_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Flatten the struct into columns
# MAGIC
# MAGIC The struct result can be unpacked with dot notation for downstream use in dashboards or tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH extracted AS (
# MAGIC   SELECT
# MAGIC     document_id,
# MAGIC     ai_extract(
# MAGIC       parsed_text,
# MAGIC       '["vendor_name","invoice_number","invoice_date","total_amount","currency_code","payment_terms"]'
# MAGIC     ) AS invoice
# MAGIC   FROM demo_invoice_text
# MAGIC )
# MAGIC SELECT
# MAGIC   document_id,
# MAGIC   invoice.vendor_name,
# MAGIC   invoice.invoice_number,
# MAGIC   invoice.invoice_date,
# MAGIC   invoice.total_amount,
# MAGIC   invoice.currency_code,
# MAGIC   invoice.payment_terms
# MAGIC FROM extracted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected output
# MAGIC
# MAGIC | document_id | vendor_name | invoice_number | invoice_date | total_amount | currency_code | payment_terms |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | INV-001 | Acme Supplies Ltd | INV-2026-0042 | 2026-04-15 | 1245.00 | USD | Net 30 |
# MAGIC | INV-002 | Schmidt GmbH | RG-2026-0099 | 2026-04-15 | 915.00 | EUR | 14 Tage netto |
# MAGIC | INV-003 | Pacific Tech Solutions | PTS-00711 | 2026-04-16 | 5280.00 | AUD | null |
# MAGIC | INV-004 | Dupont Industrie | F-2026-155 | 2026-04-17 | 1680.00 | EUR | null |
# MAGIC | INV-005 | Apex Consulting Group | ACG-2026-Q2-003 | 2026-04-18 | 16535.40 | USD | null |
# MAGIC
# MAGIC Note: The model handles English, German, French, and Australian English invoices in a single query - no language detection step required.
# MAGIC
# MAGIC ## What to do next
# MAGIC - Replace `demo_invoice_text` with `read_files('s3://your-bucket/invoices/', format => 'binaryFile')` and add `ai_parse_document(content)` to read real PDFs
# MAGIC - Add a `MERGE INTO` to land results in a gold table
# MAGIC - Tag the job with `spark.databricks.job.id` in your cluster policy to enable cost attribution via `system.billing.usage`