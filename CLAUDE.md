# CCEF Connections

Reusable Python library providing unified connection management for CCEF data integrations. Connectors: Airtable, OpenAI/ChatGPT, Google Sheets, BigQuery, HelpScout, Zoom, Action Network. Uses Civis-compatible {CREDENTIAL_NAME}_PASSWORD env var pattern. No ActionBuilder connector yet.

## PII / Data Handling

Row-level PII (names, emails, phones, street addresses, gift amounts) **never gets
committed to git** — repos here are org-visible via shared corpora and export pipelines.
Any directory that will receive raw dumps or query results gets gitignored BEFORE the
first file lands (allowlist known-clean file types; never enumerate known-bad files).
Committed derivatives must be masked or aggregated; fabricate example rows in docs.
Row-level people-data lives in access-controlled systems (BigQuery, ROI, Action Network,
shared Sheets) — point at it, don't copy it. Full policy: knowledge library entry
`pii-handling-policy` (`kl_get`).
