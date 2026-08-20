# CCEF Connections

Reusable Python library providing unified connection management for CCEF data integrations. Connectors: Airtable, OpenAI/ChatGPT, Google Sheets, BigQuery, Snowflake, HelpScout, Zendesk, Zoom, Action Network, Action Builder, Asana, PTV, ROI CRM, Stripe, Geocodio, GitHub, Hex, Email (Resend), Tatango (SMS). Uses Civis-compatible {CREDENTIAL_NAME}_PASSWORD env var pattern.

## Retry predicates: 429 only

Every service retry decorator in `core/retry.py` retries rate limiting and nothing
else. **Never put bare `Exception` in a retry predicate** — it silently retries auth
failures, 404s, non-idempotent writes, and bugs in our own code, and because
`reraise=True` still surfaces the right exception the only symptom is unexplained
slowness. Six decorators carried it until 2026-08-20; the suite was spending tens of
minutes in `time.sleep` and still passing. Full rationale in the header comment of
`core/retry.py`. Tests that exercise a decorated retry path must patch
`tenacity.nap.time.sleep`; `pytest-timeout` caps every test at 10s to catch the ones
that don't.

## Zendesk connector — read by default, narrow writes

`ZendeskConnector` defaults to OAuth scope `read`. CCEF's Zendesk is a *shared*
instance — Campaigns lives inside IT's — so read and write are separated by
credential, not convention.

Writes exist but are deliberately narrow: tickets (`create_ticket`, `update_ticket`,
`create_many_tickets`) plus single-object config writes that back reviewed
config-as-code (`create_ticket_field`, `update_ticket_field`, `update_ticket_form`,
`create_trigger`, `update_trigger`). Every config mutation targets ONE object by an
explicit id the caller had to look up. **No deletes, and nothing bulk or reconciling**
for config — an enumerate-and-write-back helper would be one bug away from clobbering
IT's production helpdesk. That surface is pinned by
`test_zendesk.py::test_write_surface_is_an_explicit_allowlist`, so widening it takes a
deliberate test edit. Namespace policy stays in the calling project.

To write, request scope `ZENDESK_READ_WRITE_SCOPE` (`"read write"`), **not** `"write"`
— Zendesk issues a token for `"write"` alone and it then 403s on every call
complaining about missing `read` scope, because even a write reads the acting user
first. Note also that a ticket field's `required` / `visible_in_portal` properties are
GLOBAL to the field, not per-form.

Do not add token/basic auth: Zendesk is removing API tokens (creation blocked
2026-10-27, all tokens dead 2027-04-30). The rate budget is per-account and shared, so
the connector self-throttles (default 120 req/min, well under the ~400/min ceiling).

## PII / Data Handling

Row-level PII (names, emails, phones, street addresses, gift amounts) **never gets
committed to git** — repos here are org-visible via shared corpora and export pipelines.
Any directory that will receive raw dumps or query results gets gitignored BEFORE the
first file lands (allowlist known-clean file types; never enumerate known-bad files).
Committed derivatives must be masked or aggregated; fabricate example rows in docs.
Row-level people-data lives in access-controlled systems (BigQuery, ROI, Action Network,
shared Sheets) — point at it, don't copy it. Full policy: knowledge library entry
`pii-handling-policy` (`kl_get`).
