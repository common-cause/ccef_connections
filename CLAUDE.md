# CCEF Connections

Reusable Python library providing unified connection management for CCEF data integrations. Connectors: Airtable, OpenAI/ChatGPT, Google Sheets, BigQuery, HelpScout, Zendesk, Zoom, Action Network, Action Builder, Asana, PTV, ROI CRM, Geocodio, GitHub, Hex, Email (Resend), Tatango (SMS). Uses Civis-compatible {CREDENTIAL_NAME}_PASSWORD env var pattern.

## Zendesk connector — read-only by default

`ZendeskConnector` defaults to OAuth scope `read` and deliberately exposes **no
config-object mutators** (triggers/macros/views/groups/forms). CCEF's Zendesk is a
*shared* instance — Campaigns lives inside IT's — so read and write are separated by
credential, not convention, and config mutation belongs with the reviewed
config-as-code apply script. Do not add token/basic auth: Zendesk is removing API
tokens (creation blocked 2026-10-27, all tokens dead 2027-04-30). The rate budget is
per-account and shared, so the connector self-throttles below the ceiling.

## PII / Data Handling

Row-level PII (names, emails, phones, street addresses, gift amounts) **never gets
committed to git** — repos here are org-visible via shared corpora and export pipelines.
Any directory that will receive raw dumps or query results gets gitignored BEFORE the
first file lands (allowlist known-clean file types; never enumerate known-bad files).
Committed derivatives must be masked or aggregated; fabricate example rows in docs.
Row-level people-data lives in access-controlled systems (BigQuery, ROI, Action Network,
shared Sheets) — point at it, don't copy it. Full policy: knowledge library entry
`pii-handling-policy` (`kl_get`).
