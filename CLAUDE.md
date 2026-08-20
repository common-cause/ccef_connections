# CCEF Connections

Reusable Python library providing unified connection management for CCEF data integrations. Connectors: Airtable, OpenAI/ChatGPT, Google Sheets, BigQuery, Snowflake, HelpScout, Zendesk, Zoom, Action Network, Action Builder, Asana, PTV, ROI CRM, Stripe, Geocodio, GitHub, Hex, Civis Platform, Email (Resend), Tatango (SMS). Uses Civis-compatible {CREDENTIAL_NAME}_PASSWORD env var pattern.

## Civis connector — the platform we run *on*

`CivisConnector` is the odd one out: every other connector reaches a system our
Civis jobs talk *to*, and this one talks to Civis itself. Consequences worth
knowing before touching it (all verified live 2026-08-20):

**The credential expires, and that is the normal end-state.** Civis caps API keys
at 30 days and offers no service-account path, so anything built on this
connector dies on a schedule. A 401 therefore means "expired" far more often than
"misconfigured", which is why `_raw` says so in the error text. Don't add 401 to a
retry predicate. `api_key_status()` reads the live `expiresAt` back from
`GET /users/{id}/api_keys` — use it rather than tracking the date in config.
Beware the two-key case: a long-lived account accumulates dead keys (the real
account had a 2020 one next to the live one), so the method considers only active,
unexpired keys, picks the soonest expiry, and sets `ambiguous` when it can't be
sure which key authenticated the request.

**`GET /jobs` is the cheap listing; `/scripts` is the expensive one.** Only
`/jobs` carries `schedule`, `state` and `lastRun` inline, so
`list_jobs(scheduled=True)` answers "what runs on Civis and did it work" in a
single request. Building the same picture from `/scripts` costs one GET per
script, and CC's own user has 666 of them against a **1000 request/hour** budget.
A full picture is `list_scheduled_jobs()` **plus** `list_workflows(scheduled=True)`
— a workflow carries the schedule and its member jobs usually don't.

**`limit` means items, not page size.** `_paginate` treats it as a total cap and
derives page size from it. This was a bug first: `list_runs(job, limit=3)`
returned all 30 runs of a daily job because `limit` was being passed straight
through as page size while pagination kept walking.

**Resource peaks are only on the container endpoint.** `/jobs/{id}/runs` returns
a minimal run object; `/scripts/containers/{id}/runs` adds `maxMemoryUsage` and
`maxCpuUsage`. That's `container_runs()` vs `list_runs()`, and the peaks are the
entire basis for sizing a dbt job — don't "simplify" the two into one.

**Run log `level` is a stream tag, not a severity.** Civis labels anything a
container wrote to stderr as `"error"`, and Python's `logging` writes INFO to
stderr, so a perfectly clean run's INFO lines all come back as errors. Judge a
run by its `state`. Any code that counts `level == "error"` to decide health is
wrong.

**Schedules fire in the owning account's timezone**, exposed as `timeZone` on the
object (CC's jobs: `America/New_York`). Not UTC, not a platform default. Always
render a schedule with its zone — `describe_schedule(sched, tz)` takes it for
exactly this reason. `describe_schedule` also handles the two forms that are easy
to miss: `scheduledRunsPerHour` (interval) and `scheduledDaysOfMonth` (monthly,
where `scheduledDays` is empty — that's how CC's "AN/AB Tables Monthly Audit"
went missing from the schedule rollup).

**Full CRUD, deliberately, on a platform we don't own.** This was an explicit
call by Rob, made knowing the key holds `manage` and the tenant is shared with 84
users across many TMC member orgs whose admins are TMC staff, not ours. So the
guardrails are conventions, not walls, and they matter:

- `is_mine(obj)` before any write — listing endpoints return neighbours' objects.
- `set_job_archived()` / `set_workflow_archived()` over `delete_container()`.
  Archiving is reversible and keeps run history; Civis itself deprecates DELETE
  on containers in favour of the archive endpoints.
- **Never PATCH `dockerCommand`.** Every CCEF job is GitHub-backed: Civis clones
  the project repo into `app/` and the body is `bash app/civis/<script>.sh`. The
  committed `.sh` is the real job — change behaviour by pushing, not by API.
- `clone_*` defaults all three clone flags to False, so a clone can't silently
  start running on the original's schedule.
- `test_civis.py::TestSurface::test_write_surface_is_pinned` pins the mutator
  inventory. It doesn't restrict what can be added; it means nobody widens the
  blast radius by accident.

**Stray-package trap.** An empty `civis/resources/` directory in a Python install
root makes `import civis` succeed as an empty namespace package —
`civis.APIClient` then raises `AttributeError`. This connector doesn't use the
official client at all, so it's immune, but that's the explanation when someone
else's script fails that way.

## Retry predicates: 429 only

Every service retry decorator in `core/retry.py` retries rate limiting (429) and
nothing else — no exceptions, as of 0.10.0. **Never put bare `Exception` in a retry
predicate** — it silently retries auth failures, 404s, non-idempotent writes, and bugs
in our own code, and because `reraise=True` still surfaces the right exception the only
symptom is unexplained slowness. Six decorators carried it until 2026-08-20; the suite
was spending tens of minutes in `time.sleep` and still passing.

**Also don't put our `ConnectionError` in a predicate.** It reads like "retry transient
network failures", but the `requests`-based connectors wrap BOTH a genuine transport
failure and any 4xx/5xx response in that one class, so retrying it means retrying 404s
and bad credentials. That's what PTV did until 0.10.0.

Full rationale in the header comment of `core/retry.py`. Tests that exercise a decorated
retry path must patch `tenacity.nap.time.sleep`; `pytest-timeout` caps every test at 10s
to catch the ones that don't.

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
