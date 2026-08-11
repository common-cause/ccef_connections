# CCEF Connections

A reusable Python library for Common Cause Education Fund data integrations. Provides unified connection management for Airtable, OpenAI, Google Sheets, BigQuery, HelpScout, Zoom, Action Network, Action Builder, Asana, Protect the Vote (PTV), ROI CRM, Geocodio, GitHub, Hex, Resend (transactional email), and Tatango (SMS) with Civis credential compatibility.

## Features

- **Airtable Integration**: Automatic retry, batch operations, formula filtering, base-schema metadata (`get_base_schema`, `list_bases`)
- **OpenAI/ChatGPT**: Langchain integration with structured outputs
- **Google Sheets**: Read-only configuration management (`SheetsConnector`) plus read/write spreadsheet publishing (`SheetsWriterConnector`)
- **BigQuery**: Full read/write data warehouse operations
- **HelpScout**: Automated email processing — read conversations, reply, add notes, close
- **Zoom**: Meeting and webinar attendee retrieval — participants, registrants, absentees
- **Action Network**: Full CRM access — people, tags, events, petitions, forms, fundraising, messages, and more
- **Action Builder**: Field organizing and relationship mapping — campaigns, people/entities, tags, taggings, and connections
- **Asana**: Read-only project and task access — tasks with custom fields, projects, sections, and workspaces, built for snapshot syncs
- **Protect the Vote (PTV)**: Election protection shift data — volunteer signups, registered volunteers, and shift availability across all states
- **ROI CRM**: Fundraising CRM — donors, donations, pledges, memberships, payment tokens, orders, contact info, and code tables
- **Geocodio**: Address geocoding — forward, reverse, and batch (up to 10,000 per request) for US, Canada, and Mexico
- **GitHub**: File-write access to a repository via the REST contents API — idempotent commits suitable for "data sync -> JSON file -> GitHub Pages" patterns
- **Hex**: Notebook/dashboard platform API — projects, full cell CRUD, and run triggering; the transport layer under the `hex-toolkit` library
- **Email (Resend)**: Transactional email — magic-links, notifications — via Resend's HTTP API
- **Tatango**: SMS broadcast platform (Messaging API v2) — subscribers with opt-in bypass flags, per-list custom fields, and webhook registrations
- **Unified Credentials**: `{CREDENTIAL_NAME}_PASSWORD` pattern for Civis compatibility
- **Automatic Retry**: Built-in exponential backoff for all APIs
- **Configuration as Code**: Manage settings via Google Sheets

## Installation

Dependencies are split by connector. The base install is lightweight
(`requests`, `tenacity`, `python-dotenv`) and covers core plus all REST
connectors: Action Builder, Action Network, Asana, Email (Resend), Geocodio,
GitHub, HelpScout, Hex, PTV, ROI CRM, Tatango, and Zoom. Heavier connectors are opt-in via extras:

| Extra | Enables | Pulls in |
|---|---|---|
| `airtable` | AirtableConnector | pyairtable |
| `sheets` | SheetsConnector, SheetsWriterConnector, ConfigManager | gspread, google-api-python-client, google-auth |
| `bigquery` | BigQueryConnector | google-cloud-bigquery, db-dtypes |
| `openai` | OpenAIConnector | langchain, langchain-openai, pydantic |
| `pandas` | DataFrame methods on BigQueryConnector | pandas |
| `all` | everything above | |

Connectors are imported lazily — importing the package never loads
dependencies you haven't installed. If you import a connector whose extra is
missing, you get an `ImportError` with the exact `pip install` command to fix it.

### From Another Project

```bash
# Base install (REST connectors only) — editable from local path:
pip install -e ../ccef-connections
# With specific extras (quote the path+extras together):
pip install -e "../ccef-connections[bigquery,sheets]"
# Example — full OneDrive path:
pip install -e "C:/Users/RobKerth/OneDrive - Common Cause Education Fund/Documents/AI Interpretation/ccef-connections[bigquery,sheets]"

# Or after publishing to PyPI
pip install "ccef-connections[bigquery,sheets]"
```

### Development Installation

```bash
# Clone or navigate to the repository
cd ccef-connections

# Install in editable mode with all extras plus dev tools
pip install -e ".[dev]"
```

## Quick Start

### Credential Setup

Create a `.env` file or set environment variables using the `{NAME}_PASSWORD` pattern:

```bash
# .env file
AIRTABLE_API_KEY_PASSWORD=keyXXXXXXXXXXXXXX
OPENAI_API_KEY_PASSWORD=sk-XXXXXXXXXXXXXXXX
GOOGLE_SHEETS_CREDENTIALS_PASSWORD={"type":"service_account",...}
BIGQUERY_CREDENTIALS_PASSWORD={"type":"service_account",...}
HELPSCOUT_CREDENTIALS_PASSWORD={"app_id":"your-app-id","app_secret":"your-app-secret"}
ZOOM_CREDENTIALS_PASSWORD={"account_id":"your-account-id","client_id":"your-client-id","client_secret":"your-client-secret"}
ACTION_NETWORK_API_KEY_PASSWORD=your-action-network-api-key
ACTION_BUILDER_CREDENTIALS_PASSWORD={"api_token":"your-api-token","subdomain":"yourorg"}
ASANA_API_KEY_PASSWORD=your-asana-personal-access-token
PTV_API_KEY_PASSWORD=your-ptv-api-key
ROI_CRM_CREDENTIALS_PASSWORD={"client_id":"your-client-id","client_secret":"your-client-secret","audience":"https://app.roicrm.net/api/1.0","roi_client_code":"YOUR_ORG"}
GEOCODIO_API_KEY_PASSWORD=your-geocodio-api-key
GITHUB_PAT_PASSWORD=ghp_XXXXXXXXXXXXXXXX
HEX_API_KEY_PASSWORD=your-hex-personal-access-token
RESEND_API_KEY_PASSWORD=re_XXXXXXXXXXXXXXXX
TATANGO_LOGIN_EMAIL_PASSWORD=api-user@yourorg.org
TATANGO_API_KEY_PASSWORD=your-tatango-api-key
RESEND_FROM_EMAIL=Your Name <auth@mail.commoncause.org>  # optional default sender
```

`RESEND_FROM_EMAIL` is optional — it provides a default sender for `EmailConnector.send()`
when no `from_addr=` is passed. The sending domain must be verified in Resend.

For GitHub, use per-repo PATs by naming the credential after the project (e.g.
`DYNAMIC_ACTION_MAP_GITHUB_PAT_PASSWORD`) and passing the matching `credential_name`
to the `GitHubConnector` constructor. See the GitHub example below.

### Airtable Example

```python
from ccef_connections import AirtableConnector

# Initialize connector (loads credentials automatically)
airtable = AirtableConnector()

# Get a table
table = airtable.get_table('appSBBlMCcLRWd2bk', 'Test Input')

# Query records
pending = table.all(formula="{status} = 'pending'")

# Update a record
airtable.update_record('appXXX', 'TableName', 'recXXX', {
    'Status': 'processed',
    'Summary': 'Done'
})
```

### OpenAI Example

```python
from ccef_connections import OpenAIConnector
from pydantic import BaseModel

class Analysis(BaseModel):
    sentiment: str
    summary: str

# Initialize connector
openai = OpenAIConnector()

# Get chat model
llm = openai.get_chat_model("gpt-4o", temperature=0.1)

# Use structured output
result = openai.invoke_with_structured_output(
    model="gpt-4o",
    system_prompt="You are a helpful assistant.",
    user_content="Analyze: I love this product!",
    response_model=Analysis
)

print(result.sentiment, result.summary)
```

### Google Sheets Example

```python
from ccef_connections import SheetsConnector

# Initialize connector
sheets = SheetsConnector()

# Read configuration as dictionaries
config = sheets.get_worksheet_as_dicts('SPREADSHEET_ID', 'Config')

for row in config:
    print(row['Section'], row['Key'], row['Value'])
```

### BigQuery Example

```python
from ccef_connections import BigQueryConnector
import pandas as pd

# Initialize connector
bq = BigQueryConnector(project_id='your-gcp-project')

# Query data
df = bq.query_to_dataframe("""
    SELECT * FROM dataset.table
    WHERE date > '2024-01-01'
    LIMIT 100
""")

# Insert rows
rows = [
    {'name': 'John', 'age': 30},
    {'name': 'Jane', 'age': 25}
]
bq.insert_rows('dataset.users', rows)

# Load DataFrame
new_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
bq.load_dataframe(new_df, 'dataset.table', if_exists='append')
```

### HelpScout Example

```python
from ccef_connections import HelpScoutConnector

# Initialize connector (OAuth2 token fetched automatically)
helpscout = HelpScoutConnector()

# List mailboxes
mailboxes = helpscout.list_mailboxes()
for mb in mailboxes:
    print(mb['id'], mb['name'])

# List active conversations in a mailbox
conversations = helpscout.list_conversations(
    mailbox_id=12345, status='active'
)

# Read threads (messages) in a conversation
threads = helpscout.list_threads(conversation_id=98765)
for thread in threads:
    print(thread.get('body', ''))

# Reply (customer_id required — get from get_conversation() → primaryCustomer → id)
helpscout.reply_to_conversation(98765, "Thanks for reaching out!", customer_id=12345)
helpscout.add_note(98765, "Resolved via automation.")
helpscout.update_conversation_status(98765, 'closed')
```

### Zoom Example

```python
from ccef_connections import ZoomConnector

# Initialize connector (Server-to-Server OAuth token fetched automatically)
zoom = ZoomConnector()

# List past meetings for a user
meetings = zoom.list_meetings("me", meeting_type="previous_meetings")

# Get attendee list from a past meeting
participants = zoom.get_past_meeting_participants("12345678901")
for p in participants:
    print(p["name"], p["user_email"], p["duration"])

# List webinars and get attendees
webinars = zoom.list_webinars("me")
attendees = zoom.get_past_webinar_participants("99887766554")

# Get registrants and absentees for a webinar
registrants = zoom.get_webinar_registrants(99887766554)
absentees = zoom.get_webinar_absentees("webinar-uuid")
```

### Action Network Example

```python
from ccef_connections import ActionNetworkConnector

# Initialize connector (API key loaded automatically, auto-connects on first call)
an = ActionNetworkConnector()

# Create or update a person (deduplicates by email — safe to call repeatedly)
person = an.create_person(
    email="activist@example.com",
    given_name="Jane",
    family_name="Doe",
    tags=["volunteer", "2026"],       # Inline tagging via Person Signup Helper
)

# Extract the person's self-link URI (needed for tagging and other cross-references)
person_uri = person["_links"]["self"]["href"]
# e.g. "https://actionnetwork.org/api/v2/people/d91b4b2e-..."

# Look up a person by email using OSDI filter syntax
results = an.list_people(filter="email_address eq 'activist@example.com'")

# Tag an existing person (person_identifiers must be full URI strings)
tag = an.create_tag("new-campaign")
tag_id = tag["identifiers"][0].split(":")[-1]   # extract UUID from "action_network:uuid"
an.add_tagging(tag_id, [person_uri])

# Events
an.create_event("Town Hall", start_date="2026-04-01T18:00:00Z")
```

### Action Builder Example

```python
from ccef_connections import ActionBuilderConnector

# Initialize connector (credentials loaded automatically, auto-connects on first call)
ab = ActionBuilderConnector()

# List all campaigns accessible to the API token
campaigns = ab.list_campaigns()
campaign_id = campaigns[0]["id"]

# List people/entities in a campaign
people = ab.list_people(campaign_id)

# Fetch people modified since a given date
recent = ab.list_people(campaign_id, modified_since="2026-01-01T00:00:00")

# Create a person
person = ab.create_person(
    campaign_id,
    given_name="Jane",
    family_name="Doe",
    email_addresses=[{"address": "jane@example.com"}],
)
person_id = person["id"]

# List tags and create a new one
tags = ab.list_tags(campaign_id)
tag = ab.create_tag(campaign_id, name="Volunteer", section="Status", field_type="checkbox")
tag_id = tag["id"]

# List and remove taggings
taggings = ab.list_taggings(campaign_id, tag_id)
person_taggings = ab.list_person_taggings(campaign_id, person_id)
ab.delete_tagging(campaign_id, tag_id, taggings[0]["id"])

# List connections for a person and mark one inactive
connections = ab.list_connections(campaign_id, person_id)
ab.update_connection(campaign_id, person_id, connections[0]["id"], inactive=True)
```

### Asana Example

```python
from ccef_connections import AsanaConnector

# PAT loaded from ASANA_API_KEY_PASSWORD; connect() validates it via GET /users/me
with AsanaConnector() as asana:
    # Find the workspace and its projects
    workspaces = asana.get_workspaces()
    projects = asana.get_projects(workspaces[0]["gid"], archived=False)

    # Pull every task in a project with full fields (DEFAULT_TASK_FIELDS),
    # including custom fields — each custom field carries a display_value
    # string rendering, the recommended consumption path for syncs
    tasks = asana.get_project_tasks(projects[0]["gid"])

    # Incremental pull: only tasks modified since a given time
    recent = asana.get_project_tasks(
        projects[0]["gid"], modified_since="2026-07-01T00:00:00Z"
    )

    # Sections and subtasks
    sections = asana.get_sections(projects[0]["gid"])
    subtasks = asana.get_subtasks(tasks[0]["gid"])
```

### Hex Example

```python
from ccef_connections import HexConnector

# PAT loaded from HEX_API_KEY_PASSWORD
with HexConnector() as hx:
    # NOTE: on a shared workspace the project list includes other member
    # orgs' projects — filter by owner before anything write-shaped
    mine = [p for p in hx.list_projects() if p["owner"]["email"] == "rkerth@commoncause.org"]

    # Read every cell's source (SQL/code/markdown) in a project
    cells = hx.list_cells(mine[0]["id"])

    # Edit a SQL cell's source in the project DRAFT (never auto-publishes)
    sql = next(c for c in cells if c["cellType"] == "SQL")
    hx.update_cell(sql["id"], {"sqlCell": {
        "source": "SELECT 1", "outputDataframe": sql["contents"]["sqlCell"]["outputDataframe"],
    }})

    # Trigger a refresh of the published app
    run = hx.run_project(mine[0]["id"])
```

Higher-level round-trip workflows (YAML export/import via the Hex CLI, git-versioned
per-cell extraction, publish guardrails) live in the `hex-toolkit` library, which
consumes this connector.

### Protect the Vote (PTV) Example

```python
from ccef_connections import PTVConnector

# Initialize connector (loads PTV_API_KEY_PASSWORD automatically)
ptv = PTVConnector()

# Fetch volunteer signups for a single state
signups = ptv.get_shift_volunteers("PA")
# Returns list of dicts: shift_id, inserted_at, date, start_time, end_time,
# timezone, locations, county, first_name, last_name, phone_number, email, role, source

# Fetch all registered volunteers for a single state
volunteers = ptv.get_users("GA")
# Returns list of dicts: id, email, join_date, phone_number, first_name,
# last_name, county, zip_code, source_code, regional_admin, shifted, training, role

# Fetch shift availability and fill rates for a single state
shifts = ptv.get_state_shifts("AZ")
# Returns list of dicts: id, date, start_time, end_time,
# locations_string, volunteers, filled

# Fetch across all states — 'state' key is added to each row automatically
state_list = ["PA", "GA", "AZ", "NV", "WI", "MI"]

all_signups = ptv.get_all_shift_volunteers(state_list)
all_volunteers = ptv.get_all_users(state_list)
all_shifts = ptv.get_all_state_shifts(state_list)

# Context manager usage
with PTVConnector() as ptv:
    signups = ptv.get_all_shift_volunteers(state_list)
```

**Note:** When a state has no data, the PTV API returns a JSON error body instead of CSV. The connector handles this transparently and returns an empty list for that state.

### ROI CRM Example

```python
from ccef_connections import ROICRMConnector

# Initialize connector (OAuth2 token fetched automatically via Auth0)
roi = ROICRMConnector()

# Search for a donor by name or email (note the API's field names:
# name_last / name_first, not last_name / first_name)
donors = roi.search_donors(name_last="Smith", email="jane@example.com")
donor_id = donors[0]["roi_family_id"]   # individual donor ID (roi_id = household ID)

# Get full donor record
donor = roi.get_donor(donor_id)

# Get donation summary and history
summary = roi.get_donation_summary(donor_id)
donations = roi.list_donations(donor_id)

# Create a donation
new_donation = roi.create_donation(donor_id, amount=100.00, fund_code="GEN")

# Work with pledges
pledges = roi.list_pledges(donor_id)
new_pledge = roi.create_pledge(donor_id, amount=25.00, frequency="monthly")
roi.update_pledge(donor_id, new_pledge["pledge_id"], amount=50.00)

# Look up memberships
memberships = roi.list_memberships(donor_id)
mvault = roi.get_mvault(donor_id)

# Add a comment and flag
roi.add_comment(donor_id, text="Spoke with donor about major gift opportunity.")
roi.add_donor_flag(donor_id, flag_code="MAJOR_PROSPECT")

# Look up valid codes for a resource type
fund_codes = roi.get_codes("donations")

# Context manager usage
with ROICRMConnector() as roi:
    donors = roi.search_donors(zip="20001")
```

### Geocodio Example

```python
from ccef_connections import GeocodioConnector

# Initialize connector
geo = GeocodioConnector()

# Single forward geocode (address → lat/lng)
result = geo.geocode("1600 Pennsylvania Ave NW, Washington, DC")
loc = result["results"][0]["location"]
print(loc["lat"], loc["lng"])  # 38.897675 -77.036548

# With field appends (congressional district, state legislature, census, etc.)
result = geo.geocode("350 Fifth Ave, New York, NY", fields=["cd", "stateleg"])

# Single reverse geocode (lat/lng → address)
result = geo.reverse_geocode(38.897675, -77.036548)
print(result["results"][0]["formatted_address"])

# Batch forward geocode — list of addresses (up to 10,000 per request)
results = geo.batch_geocode([
    "1600 Pennsylvania Ave NW, Washington, DC",
    "350 Fifth Ave, New York, NY",
])
for item in results["results"]:
    print(item["query"], item["response"]["results"][0]["location"])

# Batch with a dict — keys are preserved in the response for easy lookup
results = geo.batch_geocode({
    "whitehouse": "1600 Pennsylvania Ave NW, DC",
    "empire_state": "350 Fifth Ave, NY",
})

# Batch reverse geocode
results = geo.batch_reverse_geocode(["38.897675,-77.036548", "40.748441,-73.996277"])

# Context manager
with GeocodioConnector() as geo:
    result = geo.geocode("1600 Pennsylvania Ave NW, DC")
```

### GitHub Example

```python
from ccef_connections import GitHubConnector

# Default credential — reads GITHUB_PAT_PASSWORD
with GitHubConnector() as gh:
    # Idempotent: only commits if the file's bytes actually differ from what's
    # already in the repo. Returns the new commit SHA, or None on no-op.
    commit_sha = gh.put_file_if_changed(
        repo="common-cause/dynamic-action-map",
        path="data/states.json",
        content_bytes=b'{"key": "value"}\n',
        message="Daily sync from Google Sheet",
        branch="main",
    )
    if commit_sha:
        print(f"Pushed {commit_sha[:7]}")

# Per-repo PAT — recommended pattern. Reads DYNAMIC_ACTION_MAP_GITHUB_PAT_PASSWORD.
# Each script holds a scope-minimized token, so a leak only affects one repo.
with GitHubConnector(credential_name="DYNAMIC_ACTION_MAP_GITHUB_PAT") as gh:
    gh.put_file_if_changed(...)

# Lower-level helpers if you need them
with GitHubConnector() as gh:
    existing = gh.get_file("owner/repo", "path/to/file.json")
    if existing is None:
        print("File does not exist")
    else:
        print(f"Current sha: {existing['sha']}")

    # Explicit create or update — caller manages the SHA
    sha = gh.put_file(
        "owner/repo", "path/to/file.json",
        content_bytes=b"...",
        message="...",
        sha=existing["sha"] if existing else None,
    )
```

**Token setup:** Use a fine-grained PAT with `Contents: Read & Write` on the target
repo (and nothing else). Pin its expiration date to your calendar — fine-grained
PATs max out at 1 year and will silently break the sync when they expire.

### Email Example

```python
from ccef_connections import EmailConnector

# Reads RESEND_API_KEY_PASSWORD; from_addr falls back to RESEND_FROM_EMAIL
with EmailConnector() as email:
    # Send an HTML email — returns Resend's response dict (includes the message id)
    result = email.send(
        to="director@example.org",
        subject="Your sign-in link",
        html='<a href="https://app/auth/magic?token=...">Sign in</a>',
        from_addr="EP Roving Review <auth@mail.commoncause.org>",
    )
    print(result["id"])

    # Plain text, multiple recipients, and a reply-to
    email.send(
        to=["a@example.org", "b@example.org"],
        subject="Shift reminder",
        text="Your review shift starts at 9am.",
        reply_to="coordinator@commoncause.org",
    )
```

**Sender setup:** The from-address domain must be verified in Resend (or use Resend's
shared sending domain). Provide at least one of `html=` or `text=`. On a persistent
rate limit (429), `send()` retries with exponential backoff before raising `RateLimitError`.

### Tatango Example

```python
from ccef_connections import TatangoConnector

# Reads TATANGO_LOGIN_EMAIL_PASSWORD + TATANGO_API_KEY_PASSWORD (HTTP Basic).
# All operations are per-list; set the list once here or pass list_id per call.
with TatangoConnector(default_list_id="123456") as tatango:
    # Add a subscriber. Without bypass flags this is DOUBLE opt-in (Tatango
    # texts a confirmation the person must answer YES to). With both flags
    # it is silent and immediately subscribed; with bypass_opt_in_response
    # False the list's welcome/response message is still sent.
    result = tatango.add_subscriber(
        "3125550123",                      # bare 10-digit, no country code
        first_name="Jane",
        custom_fields={"membership_status": "Active"},
        bypass_opt_in_process=True,
        bypass_opt_in_response=True,
    )
    # ⚠️ A refused add still returns HTTP 201 — the refusal (e.g. the ~48h
    # re-subscribe cooldown) lives only in the response's status string.
    print(result.get("status"))

    # Selective in-place update (omitted fields untouched)
    tatango.update_subscriber("3125550123", {"membership_status": "Lapsed"})

    # Soft opt-out — fires the unsubscribe webhook (indistinguishable from
    # an organic STOP) and starts the ~48h re-subscribe cooldown
    tatango.delete_subscriber("3125550123")

    # Per-list custom-field schema and webhook registrations
    tatango.create_custom_field("mrc_date", "MRC Date", "datetime")
    tatango.create_webhook("https://receiver.example.org/tt?secret=...")
```

**Pacing:** Tatango's rate-limit tier is unpublished; the connector spaces requests
`min_request_interval` seconds apart (default 3.0 — the live-tested-safe rate).

### Configuration Management Example

```python
from ccef_connections import ConfigManager

# Initialize with Google Sheets config
config_mgr = ConfigManager(sheets_id='YOUR_SPREADSHEET_ID')

# Get all configuration
config = config_mgr.get_config()
base_id = config['airtable']['base_id']
model = config['openai']['model']

# Get specific value with default
temperature = config_mgr.get('openai', 'temperature', default=0.1)

# Refresh from Sheets
config_mgr.refresh()
```

**Expected Google Sheets Structure:**

| Section    | Key        | Value              | Description                    |
|------------|------------|--------------------|--------------------------------|
| airtable   | base_id    | appSBBlMCcLRWd2bk  | Airtable base ID               |
| airtable   | table_name | Test Input         | Table name for messages        |
| bigquery   | project_id | your-gcp-project   | GCP project for BigQuery       |
| bigquery   | dataset    | volunteer_data     | Dataset for storing results    |
| openai     | model      | gpt-4o             | Default ChatGPT model          |
| openai     | temperature| 0.1                | Temperature for LLM calls      |

## Migrating Existing Code

### Before (existing process_messages.py):

```python
def get_credentials():
    airtable_key = os.getenv('AIRTABLE_API_KEY_PASSWORD')
    openai_key = os.getenv('OPENAI_API_KEY_PASSWORD')
    # ... validation
    return airtable_key

def main():
    airtable_key = get_credentials()
    api = Api(airtable_key)
    table = api.table(BASE_ID, TABLE_NAME)
    # ...
```

### After (using ccef-connections):

```python
from ccef_connections import AirtableConnector, OpenAIConnector

# Initialize connectors (automatic credential loading)
airtable = AirtableConnector()
openai = OpenAIConnector()

def main():
    # Get table using connector
    table = airtable.get_table(BASE_ID, TABLE_NAME)

    # Query for pending records (unchanged from here)
    formula = "{processing_status} = 'pending'"
    pending_records = table.all(formula=formula)
    # ... rest of code unchanged
```

## Architecture

### Credential Management

All credentials follow the `{CREDENTIAL_NAME}_PASSWORD` naming convention:

- `AIRTABLE_API_KEY_PASSWORD` — API key string
- `OPENAI_API_KEY_PASSWORD` — API key string
- `GOOGLE_SHEETS_CREDENTIALS_PASSWORD` — service account JSON
- `BIGQUERY_CREDENTIALS_PASSWORD` — service account JSON
- `HELPSCOUT_CREDENTIALS_PASSWORD` — JSON with `app_id` and `app_secret`
- `ZOOM_CREDENTIALS_PASSWORD` — JSON with `account_id`, `client_id`, and `client_secret`
- `ACTION_NETWORK_API_KEY_PASSWORD` — API key string
- `ACTION_BUILDER_CREDENTIALS_PASSWORD` — JSON with `api_token` and `subdomain`
- `ASANA_API_KEY_PASSWORD` — Personal Access Token string (PATs work on all Asana plan tiers and inherit the project access of the user they belong to)
- `PTV_API_KEY_PASSWORD` — API key string
- `ROI_CRM_CREDENTIALS_PASSWORD` — JSON with `client_id`, `client_secret`, `audience`, and `roi_client_code`
- `GEOCODIO_API_KEY_PASSWORD` — API key string
- `GITHUB_PAT_PASSWORD` — Personal Access Token string (default name). Override with `GitHubConnector(credential_name="...")` to use per-repo tokens like `DYNAMIC_ACTION_MAP_GITHUB_PAT_PASSWORD`.
- `HEX_API_KEY_PASSWORD` — Personal Access Token string (workspace admin must have API access enabled; tokens created under user settings → API keys)
- `RESEND_API_KEY_PASSWORD` — API key string (plus optional `RESEND_FROM_EMAIL` for a default sender — not a `_PASSWORD` credential, just a plain env var)
- `TATANGO_LOGIN_EMAIL_PASSWORD` + `TATANGO_API_KEY_PASSWORD` — Tatango authenticates with HTTP Basic as `login email : API key`, so both ride as credentials (the email isn't secret, but Civis carries it the same way)

This pattern is compatible with Civis Docker environments while also working seamlessly in local development with `.env` files.

### Retry Logic

All connectors include automatic retry with exponential backoff:

- **Airtable**: 5 retries, handles 5 req/sec rate limit
- **OpenAI**: 5 retries, handles 429 rate limit errors
- **Google APIs**: 5 retries, handles quota limits
- **HelpScout**: 5 retries, handles rate limits with auto token refresh on 401
- **Zoom**: 5 retries, handles rate limits with auto token refresh on 401
- **Action Network**: 5 retries, handles 429 rate limits (4 req/s)
- **Action Builder**: 5 retries, handles 429 rate limits (4 req/s)
- **Asana**: 5 retries on 429 rate limit only (1,500 req/min paid, 150 free), honoring the exact `Retry-After` duration the API specifies plus a 2s buffer. Other HTTP errors — including 402 for paid-tier features on a free workspace — surface immediately.
- **PTV**: 5 retries, handles transient connection errors and rate limits
- **ROI CRM**: 5 retries on 429 rate limit only (500 req per 5-min window); other HTTP errors surface immediately
- **GitHub**: 5 retries on 429 / 403 secondary rate limits, honoring the exact `Retry-After` (or `x-ratelimit-reset`) duration the API specifies plus a 2s buffer. Other HTTP errors surface immediately.
- **Geocodio**: 5 retries on 429 rate limit only; other HTTP errors surface immediately
- **Hex**: 3 retries with backoff on read calls (60 req/min limit); writes (create/update/delete cell) run single-shot — a retried POST could duplicate a cell
- **Email (Resend)**: 5 retries on 429 rate limit only; other HTTP errors surface immediately
- **Tatango**: 5 retries on 429 rate limit only; other HTTP errors (including WAF 403 body blocks) surface immediately. The connector also paces itself client-side (`min_request_interval`, default 3.0s) since the vendor tier is unpublished — and note business-level refusals arrive inside HTTP **201** bodies, which no retry logic sees
- **Transient errors**: Automatic retry for network failures

### Auto-Connect Behavior

All connectors auto-connect on first API call. You never need to call `.connect()` explicitly — just instantiate and start using methods:

```python
an = ActionNetworkConnector()        # No .connect() needed
people = an.list_people()            # Connects automatically on first call
```

Calling `.connect()` explicitly is supported but optional. Use it if you want to fail fast on missing credentials before entering a processing loop.

### Context Manager Support

All connectors support context managers for automatic cleanup:

```python
with AirtableConnector() as conn:
    table = conn.get_table('appXXX', 'Table')
    records = table.all()
# Connection automatically cleaned up
```

## API Reference

### AirtableConnector

- `get_table(base_id, table_name)` - Get a table instance
- `get_records(base_id, table_name, formula=None, ...)` - Query records with retry
- `get_base_schema(base_id)` - Full base schema (tables, fields, types) via the metadata API; needs PAT scope `schema.bases:read` + per-base access
- `list_bases()` - All bases visible to the PAT (`[{id, name, permission_level}]`); needs `schema.bases:read`
- `update_record(base_id, table_name, record_id, fields)` - Update a record
- `batch_update(base_id, table_name, records)` - Update multiple records
- `create_record(base_id, table_name, fields)` - Create a new record
- `batch_upsert(base_id, table_name, records, key_fields, replace=False)` - Upsert records matched on `key_fields` (patch existing, create missing; never deletes)

### OpenAIConnector

- `get_chat_model(model="gpt-4o", temperature=0.1)` - Get configured chat model
- `invoke_with_structured_output(model, system_prompt, user_content, response_model)` - Get structured response
- `create_prompt_template(messages)` - Create chat prompt template

### SheetsConnector

- `get_spreadsheet(spreadsheet_id)` - Get spreadsheet instance
- `get_worksheet(spreadsheet_id, worksheet_name)` - Get worksheet
- `get_range(spreadsheet_id, range_name)` - Get range values
- `get_all_values(spreadsheet_id, worksheet_name)` - Get all worksheet values
- `get_range_as_dicts(spreadsheet_id, range_name)` - Get range as list of dicts
- `get_worksheet_as_dicts(spreadsheet_id, worksheet_name)` - Get worksheet as list of dicts

### SheetsWriterConnector

Read/write Google Sheets access using the same `GOOGLE_SHEETS_CREDENTIALS_PASSWORD`
credential as `SheetsConnector`, but with write scopes (`spreadsheets` + `drive`).
Designed for "compute -> publish to a spreadsheet" jobs. Requires the `sheets` extra.

- `get_or_create_spreadsheet(title, folder_id=None)` - Open a spreadsheet by title or create it. With `folder_id`, lookup and creation are scoped to that Drive folder (bypasses the service account's own Drive quota)
- `get_or_add_worksheet(spreadsheet, title)` - Return a worksheet tab, adding it if missing
- `write_worksheet(spreadsheet, worksheet_name, data, value_input_option="RAW")` - Clear a tab and write a 2D list of values (use `"USER_ENTERED"` if data contains formulas)
- `delete_worksheet_if_exists(spreadsheet, title)` - Delete a tab if present (no-op otherwise)
- `format_header_row(spreadsheet, worksheet_name)` - Bold and freeze row 1
- `move_to_folder(spreadsheet, folder_id)` - Move a spreadsheet into a Drive folder (no-op if already there)

### BigQueryConnector

Service-account credentials are loaded with both the BigQuery and Google Drive OAuth
scopes (since v0.2.1), so queries against Drive-backed external tables (e.g. tables
defined over Google Sheets) work as long as the sheet is shared with the service account.

- `query(sql, params=None, timeout=None)` - Execute SQL query
- `query_to_dataframe(sql, params=None)` - Query to pandas DataFrame
- `table_exists(table_id)` - Check if table exists
- `insert_rows(table_id, rows)` - Streaming insert
- `load_dataframe(df, table_id, if_exists='append')` - Load DataFrame
- `execute_dml(sql)` - Execute UPDATE/DELETE statements

### HelpScoutConnector

- `list_mailboxes()` - List all mailboxes
- `list_conversations(mailbox_id, status=None, tag=None)` - List conversations with filters
- `get_conversation(conversation_id)` - Get a single conversation
- `list_threads(conversation_id)` - List all messages in a conversation
- `reply_to_conversation(conversation_id, text, customer_id, draft=False)` - Reply to a conversation (customer_id from `get_conversation()` → `primaryCustomer` → `id`)
- `add_note(conversation_id, text)` - Add an internal note
- `update_conversation_status(conversation_id, status)` - Set status via PATCH (active/pending/closed)

### ZoomConnector

- `get_user(user_id="me")` - Get user profile
- `list_meetings(user_id="me", meeting_type="scheduled")` - List meetings
- `get_meeting(meeting_id)` - Get meeting details
- `get_past_meeting_participants(meeting_id)` - Get attendees from a completed meeting
- `list_webinars(user_id="me")` - List webinars
- `get_webinar(webinar_id)` - Get webinar details
- `get_webinar_registrants(webinar_id, status="approved")` - List webinar registrants
- `get_past_webinar_participants(webinar_id)` - Get attendees from a completed webinar
- `get_webinar_absentees(webinar_id)` - Get registered no-shows
- `get_meeting_registrants(meeting_id, status="approved")` - List meeting registrants

### ActionNetworkConnector

**Important concepts:**

- **OSDI/HAL+JSON format**: All Action Network responses use this format. Resource IDs are in `identifiers` (list of strings like `"action_network:uuid"`). Self-links and related resource links are in `_links`. Nested collections are in `_embedded`.
- **Pagination**: All `list_*` methods automatically follow `_links.next.href` and return **every** record across all pages. There is no `max_results` parameter. If you have a large dataset (e.g. 50k people), always use `filter` parameters instead of listing everything.
- **Person Signup Helper**: `create_person()` uses the AN signup helper endpoint which **deduplicates by email**. Calling it with an existing email updates (merges) the record instead of creating a duplicate. The response is the same whether created or updated.
- **Tagging URIs**: `add_tagging()` requires full person URI strings (e.g. `"https://actionnetwork.org/api/v2/people/uuid"`), not bare UUIDs. Extract these from `person["_links"]["self"]["href"]`.
- **No DELETE on most resources**: Action Network does not support DELETE for people, events, petitions, etc. Use status updates instead. Taggings are the exception — `delete_tagging()` works.

**People:**

- `list_people(**filters)` - List people (paginated). Supports OSDI filter syntax: `an.list_people(filter="email_address eq 'x@y.com'")`
- `get_person(person_id)` - Get a single person by UUID
- `create_person(email, given_name=None, family_name=None, tags=None, **kwargs)` - Create/update person via signup helper. Deduplicates by email. Pass `tags=["tag1", "tag2"]` for inline tagging.
- `update_person(person_id, fields)` - Update a person (PUT — sends full replacement of provided fields)
- `unsubscribe_person(person_id)` - Unsubscribe a person by UUID (sets email status to `"unsubscribed"` via PUT). **Scoped to the API key's group** — does not affect other groups in a federated network.
- `unsubscribe_person_by_email(email)` - Unsubscribe by email address (no UUID lookup needed). Uses the Person Signup Helper (POST). If the person doesn't exist, they are added in an unsubscribed state.

**Phone / SMS ops** (phones are stored as 11 digits with leading `1`, no `+`; AN effectively holds **one phone per person**):

- `find_people_by_phone(phone_number)` - Exact-match phone lookup via `filter=phone_number eq '...'` (fast and freshly indexed, unlike `modified_date` filters)
- `get_person_phone(person_id)` - The person's phone entry (`number`, `status`, `number_type`) or `None`
- `set_person_phone_status(person_id, status)` - Set SMS status `"subscribed"`/`"unsubscribed"` via a **status-only PUT**. Never sends a number — a phone PUT carrying `number` **replaces** the person's phone (live-verified). Email status is untouched; scoped to the API key's group.

**Tags & Taggings:**

- `list_tags()` - List all tags
- `get_tag(tag_id)` - Get a tag by UUID
- `create_tag(name)` - Create a tag
- `list_taggings(tag_id)` - List taggings for a tag
- `add_tagging(tag_id, person_identifiers)` - Tag a person. `person_identifiers` must be a list of **full URI strings** like `["https://actionnetwork.org/api/v2/people/uuid"]`
- `delete_tagging(tag_id, tagging_id)` - Remove a tagging (one of the few DELETE operations AN supports)

**Events & Attendances:**

- `list_events()` / `get_event(id)` / `create_event(title, start_date=None, ...)` / `update_event(id, fields)` - Event CRUD
- `list_attendances(event_id)` / `get_attendance(event_id, id)` / `create_attendance(event_id, person_data)` - Attendances

**Petitions & Signatures:**

- `list_petitions()` / `get_petition(id)` / `create_petition(title, ...)` / `update_petition(id, fields)` - Petitions
- `list_signatures(petition_id)` / `get_signature(petition_id, id)` / `create_signature(petition_id, person_data)` / `update_signature(petition_id, id, fields)` - Signatures

**Forms & Submissions:**

- `list_forms()` / `get_form(id)` / `create_form(title, ...)` / `update_form(id, fields)` - Forms
- `list_submissions(form_id)` / `get_submission(form_id, id)` / `create_submission(form_id, person_data)` - Submissions

**Fundraising & Donations:**

- `list_fundraising_pages()` / `get_fundraising_page(id)` / `create_fundraising_page(title, ...)` / `update_fundraising_page(id, fields)` - Fundraising pages
- `list_donations(page_id)` / `get_donation(page_id, id)` / `create_donation(page_id, person_data)` - Donations

**Other resources:**

- `list_lists()` / `get_list(id)` - Lists (queries/segments, read-only)
- `list_messages()` / `get_message(id)` / `create_message(subject, body=None, targets=None)` - Email messages
- `list_wrappers()` / `get_wrapper(id)` / `create_wrapper(...)` / `update_wrapper(id, fields)` - Email wrapper templates
- `list_custom_fields()` / `get_custom_field(id)` / `create_custom_field(name, format)` / `update_custom_field(id, fields)` - Custom field definitions (metadata)
- `list_event_campaigns()` / `get_event_campaign(id)` / `create_event_campaign(title, ...)` / `update_event_campaign(id, fields)` - Event campaigns
- `list_campaign_events(campaign_id)` / `create_campaign_event(campaign_id, event_data)` - Events within campaigns

### ActionBuilderConnector

Action Builder is a relationship-mapping and field organizing platform. All resources are scoped to a campaign. The API uses OSDI v1.2.0 with page-based pagination (`page` / `per_page` / `total_pages`).

**Important concepts:**

- **Campaign-scoped**: Every method (except `list_campaigns` / `get_campaign`) requires a `campaign_id` parameter.
- **Connections are read/update only**: The API does not support creating connections — use the Connection Helper UI instead. You can list connections, fetch individual ones, and toggle `inactive` status.
- **Taggings are read/delete only**: The API does not support creating or updating taggings.
- **`modified_since` filter**: `list_campaigns()` and `list_people()` accept an optional `modified_since` ISO-8601 string that translates to an OData filter (`modified_date gt '...'`).
- **OSDI vs `action_builder:` embedded keys**: Action Builder's HAL+JSON responses use two different namespace prefixes in `_embedded`. Per the official API docs: resources defined in the OSDI standard use `osdi:` (people, tags, taggings); resources specific to Action Builder use `action_builder:` (campaigns, entity_types, connection_types, connections). This matters if you inspect raw API responses directly.

**Campaigns:**

- `list_campaigns(modified_since=None)` - List all campaigns
- `get_campaign(campaign_id)` - Get a single campaign

**Entity Types (read-only):**

- `list_entity_types(campaign_id)` - List entity types for a campaign
- `get_entity_type(campaign_id, type_id)` - Get a single entity type

**Connection Types (read-only):**

- `list_connection_types(campaign_id)` - List connection types for a campaign
- `get_connection_type(campaign_id, type_id)` - Get a single connection type

**People / Entities:**

- `list_people(campaign_id, modified_since=None, **filters)` - List all people/entities
- `get_person(campaign_id, person_id)` - Get a single person/entity
- `create_person(campaign_id, **fields)` - Create a person/entity
- `update_person(campaign_id, person_id, fields)` - Update a person/entity
- `delete_person(campaign_id, person_id)` - Delete a person/entity

**Tags:**

- `list_tags(campaign_id)` - List all tags
- `get_tag(campaign_id, tag_id)` - Get a single tag
- `create_tag(campaign_id, name, section, field_type, **kwargs)` - Create a tag/field
- `delete_tag(campaign_id, tag_id)` - Delete a tag

**Taggings (read + delete only):**

- `list_taggings(campaign_id, tag_id)` - List taggings for a tag
- `list_person_taggings(campaign_id, person_id)` - List taggings for a person
- `delete_tagging(campaign_id, tag_id, tagging_id)` - Remove a tagging

**Connections (read + update only):**

- `list_connections(campaign_id, person_id)` - List connections for a person
- `get_connection(campaign_id, person_id, connection_id)` - Get a single connection
- `update_connection(campaign_id, person_id, connection_id, inactive)` - Toggle inactive status

### AsanaConnector

Provides read-only access to Asana tasks (including custom fields), projects, sections, and workspaces — built for snapshot-sync jobs that pull whole projects into a warehouse.

**Credential:** `ASANA_API_KEY_PASSWORD` (Personal Access Token string). PATs work on all Asana plan tiers, are tied to a human user account, and inherit that user's project access. `connect()` validates the token via `GET /users/me`.

**Auth:** Bearer-token PAT on every request. Base URL: `https://app.asana.com/api/1.0`. Rate limit: 1,500 req/min on paid domains (150 free), per token; retries on 429 honor the `Retry-After` header plus a 2s buffer. Asana returns **402 Payment Required** for paid-tier endpoints or parameters used on a free workspace — the connector surfaces that as a non-retryable error with an explicit message.

- `get_workspaces()` - List all workspaces visible to the PAT
- `get_projects(workspace_gid, archived=None)` - List projects in a workspace; `archived` filters to archived (True) or active (False) projects
- `get_project(project_gid, opt_fields=None)` - Get one project's metadata (request `custom_field_settings` via `opt_fields` for its custom-field schema)
- `get_sections(project_gid)` - List a project's sections
- `get_project_tasks(project_gid, opt_fields=DEFAULT_TASK_FIELDS, modified_since=None, completed_since=None)` - The workhorse: list every task in a project with full fields; datetime filters accept ISO-8601 strings or `datetime` objects
- `get_task(task_gid, opt_fields=DEFAULT_TASK_FIELDS)` - Get a single task
- `get_subtasks(task_gid, opt_fields=DEFAULT_TASK_FIELDS)` - List a task's subtasks

**Important concepts:**

- All GIDs are opaque strings, not ints.
- Default Asana responses are compact stubs (`gid`/`name`/`resource_type`) — a useful task pull requires `opt_fields`. The module-level `DEFAULT_TASK_FIELDS` constant covers the common sync fields (name, notes, completion, dates, assignee, memberships, tags, custom fields, parent, permalink); override per call with a comma-separated string (dot notation for nested fields, e.g. `assignee.email`).
- Custom fields requested via `opt_fields=custom_fields` come back as full objects with `gid`, `name`, `type`, type-specific value fields, and `display_value` — a universal string rendering that is the recommended consumption path for syncs.
- Pagination is handled internally with offset tokens (`limit=100` per page); offset tokens are never persisted across runs.

### HexConnector

Thin REST client for the Hex public API — projects, full cell CRUD, and run triggering. Transport only: round-trip workflows (YAML export/import via the Hex CLI, git-versioned per-cell extraction, publish guardrails) live in the `hex-toolkit` library, which consumes this connector.

**Credential:** `HEX_API_KEY_PASSWORD` (Personal Access Token string). The workspace admin must have API access enabled; tokens are created under user settings → API keys. Override the credential name or pass `base_url` for a single-tenant instance via the constructor.

**Auth:** Bearer-token PAT on every request. Base URL: `https://app.hex.tech/api/v1`. Rate limit: 60 req/min per user (plus 25 concurrent kernels); 429s raise `RateLimitError` honoring `Retry-After`. Reads retry with backoff; writes run single-shot (a retried POST could duplicate a cell).

- `list_projects(params=None)` - All viewable projects, pagination followed. On a shared workspace this includes other orgs' projects — filter by owner before writing
- `get_project(project_id)` / `create_project(title, description=None)`
- `list_cells(project_id)` - Every cell with type, label, and full source (`sqlCell`/`codeCell`/`markdownCell`); INPUT/CHART/MAP cells appear with null contents
- `get_cell(cell_id)` / `get_cell_output(cell_id)`
- `create_cell(project_id, cell_type, contents, label=None, data_connection_id=None)` - Into the project draft
- `update_cell(cell_id, contents=None, data_connection_id=None)` - Draft edit; also the working path for attaching a data connection; `label` PATCHes are ignored by the API
- `delete_cell(cell_id)`
- `list_data_connections()` - Workspace connections (id, name, type)
- `run_project(project_id, options=None)` - Runs the latest PUBLISHED version
- `list_runs(project_id)` / `get_run(project_id, run_id)` / `cancel_run(project_id, run_id)`

**Important concepts:**

- Cell writes bump the cell's `id` but keep `staticId` stable — treat `staticId` as the durable identity. The Hex YAML export's `cellId` equals the API `staticId`.
- Cell edits land in the project **draft**; nothing here publishes. There is no publish endpoint in the API at all (verified 2026-07-22).
- Quirk: `dataConnectionId` on cell CREATE does not attach the connection (and a SQL cell without one fails the whole app run) — but a follow-up `update_cell(cell_id, data_connection_id=...)` PATCH attaches it fine. hex-toolkit's `create_cell` verb does the two-step automatically.

### PTVConnector

Provides read access to Protect the Vote shift scheduling data across three endpoints, all scoped per state.

**Credential:** `PTV_API_KEY_PASSWORD` (plain API key string)

**Shift volunteers** (`shift_volunteers_csv`):

- `get_shift_volunteers(state_code)` - Fetch volunteer signups for one state. Returns list of dicts with keys: `shift_id`, `inserted_at`, `date`, `start_time`, `end_time`, `timezone`, `locations`, `county`, `first_name`, `last_name`, `phone_number`, `email`, `role`, `source`
- `get_all_shift_volunteers(state_codes)` - Fetch signups across multiple states. Adds `state` key to each row.

**Registered volunteers** (`users_csv`):

- `get_users(state_code)` - Fetch all registered volunteers for one state. Returns list of dicts with keys: `id`, `email`, `join_date`, `phone_number`, `first_name`, `last_name`, `county`, `zip_code`, `source_code`, `regional_admin`, `shifted`, `training`, `role`
- `get_all_users(state_codes)` - Fetch volunteers across multiple states. Adds `state` key to each row.

**Shift availability** (`state_shifts_csv`):

- `get_state_shifts(state_code)` - Fetch all shifts and fill rates for one state. Returns list of dicts with keys: `id`, `date`, `start_time`, `end_time`, `locations_string`, `volunteers`, `filled`
- `get_all_state_shifts(state_codes)` - Fetch shifts across multiple states. Adds `state` key to each row.

### ROICRMConnector

Provides access to ROI CRM donor and fundraising data via OAuth2 Client Credentials (Auth0). Token is valid for 24 hours and refreshes automatically on expiry or 401.

**Credential:** `ROI_CRM_CREDENTIALS_PASSWORD` — JSON with `client_id`, `client_secret`, `audience`, and `roi_client_code`

**Auth:** `https://roisolutions.us.auth0.com/oauth/token` (Client Credentials grant). Base URL: `https://app.roicrm.net/api/1.0`. Rate limit: 500 requests per 5-minute rolling window.

**System:**

- `ping()` - Verify API connectivity (also used for health check)
- `get_server_time()` - Get current server time

**Donors:**

- `search_donors(**kwargs)` - Search donors by field values (e.g. `name_last`, `email`, `zip`). Result items use the API's field names: `roi_family_id` (individual ID), `roi_id` (household ID), `name_first`/`name_last` — not `id`/`first_name`/`last_name`. See the method docstring for the full field list.
- `get_donor(donor_id)` - Get a donor record by ID
- `create_donor(**kwargs)` - Create a new donor record
- `update_donor(donor_id, **kwargs)` - Update donor fields (PATCH)
- `get_donor_flextable(donor_id, table_name)` - Get a named custom field table for a donor

**Donations:**

- `get_donation_summary(donor_id, **kwargs)` - Get totals/stats (optional `start_date`/`end_date`)
- `list_donations(donor_id, **kwargs)` - List all donation transactions
- `get_donation(donor_id, txn_id)` - Get a single transaction
- `create_donation(donor_id, **kwargs)` - Record a new donation (e.g. `amount`, `fund_code`, `date`)
- `add_donation_flag(donor_id, txn_id, **kwargs)` - Add a flag to a transaction
- `get_related_transactions(donor_id, txn_id)` - List related transactions (e.g. matching gifts)
- `get_related_transaction(donor_id, txn_id, rel_id)` - Get a single related transaction
- `get_honoree_transactions(donor_id)` - List transactions where this donor is the honoree

**Pledges:**

- `list_pledges(donor_id, **kwargs)` - List all pledges
- `get_pledge(donor_id, pledge_id)` - Get a single pledge
- `create_pledge(donor_id, **kwargs)` - Create a pledge (e.g. `amount`, `frequency`, `start_date`)
- `update_pledge(donor_id, pledge_id, **kwargs)` - Update pledge fields (PATCH)
- `add_pledge_flag(donor_id, pledge_id, **kwargs)` - Add a flag to a pledge

**Payment Tokens:**

- `list_payment_tokens(donor_id)` - List stored payment tokens
- `get_payment_token(donor_id, token_id)` - Get a single token
- `create_payment_token(donor_id, **kwargs)` - Store a new payment token
- `update_payment_token(donor_id, token_id, **kwargs)` - Update a token (PATCH)

**Contact Info:**

- `get_primary_address(donor_id)` - Get primary mailing address
- `list_other_addresses(donor_id)` - List non-primary addresses
- `list_emails(donor_id)` - List all email addresses
- `list_phones(donor_id)` - List all phone numbers

**Comments & Flags:**

- `list_comments(donor_id)` - List all staff comments on a donor record
- `add_comment(donor_id, **kwargs)` - Add a comment (e.g. `text`, `date`)
- `get_comment(donor_id, comment_id)` - Get a single comment
- `list_donor_flags(donor_id)` - List all flags on a donor record
- `add_donor_flag(donor_id, **kwargs)` - Add a flag (e.g. `flag_code`)

**Memberships:**

- `list_memberships(donor_id)` - List all memberships
- `get_membership(donor_id, membership_id)` - Get a single membership
- `list_submemberships(donor_id)` - List sub-memberships
- `get_mvault(donor_id)` - Get the MVault membership record

**Orders:**

- `list_orders(donor_id)` - List all orders
- `get_order(donor_id, order_id)` - Get a single order
- `create_order(donor_id, **kwargs)` - Create a new order

**Code Tables:**

- `get_codes(entity)` - Get valid code values for a resource type (e.g. `"donations"`, `"donors"`, `"pledges"`)

### GeocodioConnector

Provides forward and reverse geocoding via the Geocodio API v1.10. Supports single-address and batch operations (up to 10,000 per request). Coverage: US and Canada (forward + reverse), Mexico (forward only).

**Credential:** `GEOCODIO_API_KEY_PASSWORD` (plain API key string)

**Auth:** `api_key` query parameter on every request. Base URL: `https://api.geocod.io/v1.10`. Retries on 429 (quota exceeded) with exponential backoff.

**Optional `fields` parameter:** Append enrichment data to any geocode result by passing a list of field names. Common values: `cd` (congressional district), `stateleg` (state legislature), `census`, `timezone`, `school`, `zip4`.

- `geocode(address, fields=None, limit=1)` - Forward geocode a single address. Returns Geocodio response dict with `input` and `results` keys.
- `reverse_geocode(lat, lng, fields=None, limit=1)` - Reverse geocode a single coordinate pair. Returns dict with `results` key.
- `batch_geocode(addresses, fields=None, limit=1)` - Batch forward geocode up to 10,000 addresses. `addresses` may be a list of strings or a dict mapping custom keys to addresses. Returns Geocodio batch response dict.
- `batch_reverse_geocode(coordinates, fields=None, limit=1)` - Batch reverse geocode up to 10,000 coordinate pairs. `coordinates` may be a list of `"lat,lng"` strings or a dict mapping custom keys to `"lat,lng"` strings.

### GitHubConnector

Provides file-write access to a GitHub repository via the REST contents API. Designed
for "data sync -> JSON file -> GitHub Pages" patterns where a scheduled job commits a
single file to a repo on every tick.

**Credential:** `GITHUB_PAT_PASSWORD` by default — a Personal Access Token string. Pass
`credential_name="..."` to the constructor to read a different credential (recommended:
one PAT per repo, e.g. `DYNAMIC_ACTION_MAP_GITHUB_PAT`).

**Auth:** Bearer-token PAT on every request. Base URL: `https://api.github.com`.
Rate limit: 5000 requests/hour for authenticated PATs; the connector also honors
GitHub's secondary rate limits (403 with `x-ratelimit-remaining: 0`).

- `get_file(repo, path, ref="main")` - Fetch a file. Returns `{"content_bytes": bytes, "sha": str}` or `None` if the file doesn't exist on `ref`. Raises `WriteError` if `path` is a directory.
- `put_file(repo, path, content_bytes, message, branch="main", sha=None)` - Create or update a file. For updates, pass the file's current SHA via `sha`. Returns the new commit SHA. Raises `WriteError` on SHA conflicts (409) or validation failures (422).
- `put_file_if_changed(repo, path, content_bytes, message, branch="main")` - The headline call site for idempotent sync jobs. Fetches the file, compares bytes, and only PUTs if there's a real change. Returns the new commit SHA, or `None` if the repo already had identical content. Safe to call on every scheduled run; no-op days produce no commits.

### EmailConnector

Sends transactional email (magic-links, notifications) via Resend's HTTP API. No live health endpoint — `health_check()` returns True when connected with a non-empty key (mirrors GeocodioConnector).

**Credential:** `RESEND_API_KEY_PASSWORD` (plain API key string). Optional `RESEND_FROM_EMAIL` env var supplies a default sender.

**Auth:** Bearer-token API key on every request. Base URL: `https://api.resend.com`. Retries on 429 (rate limit) with exponential backoff; 4xx/5xx surface immediately.

- `send(to, subject, *, html=None, text=None, from_addr=None, reply_to=None)` - Send an email. `to` and `reply_to` accept a single address or a list. `from_addr` falls back to `RESEND_FROM_EMAIL`. Requires at least one of `html`/`text`. Returns Resend's response dict (includes the message `id`). Raises `ValueError` if no sender resolves or no body is given.

### TatangoConnector

SMS broadcast platform (Tatango, now MomoGood), Messaging API v2. Base URL: `https://app.tatango.com/api/v2`. Endpoint shapes were live-verified (tatango-sync project, 2026-06 / 2026-08).

**Credentials:** `TATANGO_LOGIN_EMAIL_PASSWORD` + `TATANGO_API_KEY_PASSWORD` (HTTP Basic: `login email : API key`).

**Constructor:** `TatangoConnector(default_list_id=None, min_request_interval=3.0)` — all operations are per-list; the per-call `list_id` overrides the default. The connector paces requests `min_request_interval` seconds apart (vendor rate tier unpublished; 1 req/3s is the tested-safe rate).

**Important concepts:**

- **The `status` string is the outcome, not the HTTP code.** A *refused* subscriber add (e.g. the ~48h post-opt-out re-subscribe cooldown) still returns HTTP **201** — inspect the response's `status`, or re-GET the subscriber and check `optin_in_progress`.
- **Double opt-in is the API default** even on single-opt-in lists. `bypass_opt_in_process=True` skips it; pair with `bypass_opt_in_response=True` for a fully silent add, or `False` to also send the list's welcome/response message.
- **`delete_subscriber` is a soft opt-out** (record retained, `opted_out_at` set) and **fires the `unsubscribe` webhook** with a payload indistinguishable from an organic STOP — callers doing bidirectional sync need echo suppression (self-write ledger).
- **Timestamps** come back with inconsistent (but instant-preserving) UTC offsets — parse *with* the offset, never strip it.
- Phones are **bare 10-digit** (no country code) throughout.

**Subscribers:**

- `get_subscriber(phone_number, list_id=None)` - Read a subscriber (soft-opted-out records still return 200)
- `add_subscriber(phone_number, first_name=None, last_name=None, email=None, zip_code=None, custom_fields=None, bypass_opt_in_process=False, bypass_opt_in_response=False, list_id=None, **extra_fields)` - Opt in a subscriber; custom-field values ride as flat keys and are coerced to the field's declared type
- `update_subscriber(phone_number, fields, list_id=None)` - Selective in-place PUT (omitted fields untouched; tags are additive; datetime fields accept plain ISO dates)
- `delete_subscriber(phone_number, list_id=None)` - Soft opt-out (see above; starts the ~48h re-subscribe cooldown)
- `list_subscribers(list_id=None, **params)` - Single page, raw response (pagination shape not live-verified)

**Custom fields & webhooks (per-list):**

- `list_custom_fields(list_id=None)` / `create_custom_field(key, label, content_type, max_length=100, list_id=None)` - Schema CRUD. Create path is **plural** `/custom_fields` (documented singular path 404s) and `max_length` is **required** (422 without it). `content_type`: `text` / `datetime` / `number`
- `list_webhooks(list_id=None)` / `create_webhook(callback_url, subscribe=True, unsubscribe=True, message_sent=False, cleaned=True, reply_received=False, list_id=None)` / `delete_webhook(webhook_id, list_id=None)` - All five events settable via API. No HMAC signing (trust = URL secret + IP allowlist); duplicate delivery happens even on success — dedupe on `opt_id`

**Lists:**

- `list_lists(**params)` / `get_list(list_id=None)` - List config; useful fields `opt_in_type` (`single`/`double`) and `counts`

### ConfigManager

- `get_config()` - Get all configuration
- `get(section, key, default=None)` - Get specific value
- `refresh()` - Refresh from Google Sheets
- `clear_cache()` - Clear configuration cache

## Action Network Response Format

Action Network uses the OSDI/HAL+JSON format. All responses are dicts with a consistent structure. Here are the key shapes:

### Person (from `get_person()`, `create_person()`, items in `list_people()`)

```python
{
    "identifiers": ["action_network:d91b4b2e-ae0e-4cd3-9ed7-de9uemdse"],
    "given_name": "Jane",
    "family_name": "Doe",
    "email_addresses": [
        {"address": "jane@example.com", "primary": True, "status": "subscribed"}
    ],
    "postal_addresses": [
        {"postal_code": "20001", "country": "US", "region": "DC", ...}
    ],
    "phone_numbers": [
        {"number": "2025551234", "number_type": "Mobile", "primary": True}
    ],
    "custom_fields": {
        "district": "DC-01",
        "volunteer_level": "lead"
    },
    "created_date": "2026-01-15T10:30:00Z",
    "modified_date": "2026-02-01T14:00:00Z",
    "languages_spoken": ["en"],
    "_links": {
        "self": {"href": "https://actionnetwork.org/api/v2/people/d91b4b2e-..."},
        "osdi:taggings": {"href": "https://actionnetwork.org/api/v2/people/d91b4b2e-.../taggings"},
        "osdi:donations": {"href": "https://actionnetwork.org/api/v2/people/d91b4b2e-.../donations"},
        ...
    }
}
```

**Key fields to extract:**
- **Person self-link URI** (needed for `add_tagging`): `person["_links"]["self"]["href"]`
- **Person UUID**: `person["identifiers"][0].split(":")[-1]`
- **Primary email**: `person["email_addresses"][0]["address"]`
- **Custom fields**: `person.get("custom_fields", {})`

### Tag (from `get_tag()`, items in `list_tags()`)

```python
{
    "identifiers": ["action_network:tag-uuid-here"],
    "name": "volunteer",
    "created_date": "2026-01-01T00:00:00Z",
    "modified_date": "2026-01-01T00:00:00Z",
    "_links": {
        "self": {"href": "https://actionnetwork.org/api/v2/tags/tag-uuid-here"},
        "osdi:taggings": {"href": "https://actionnetwork.org/api/v2/tags/tag-uuid-here/taggings"}
    }
}
```

**Extract tag UUID**: `tag["identifiers"][0].split(":")[-1]`

### Event (from `get_event()`, items in `list_events()`)

```python
{
    "identifiers": ["action_network:event-uuid-here"],
    "title": "Town Hall",
    "description": "Monthly town hall meeting",
    "start_date": "2026-04-01T18:00:00Z",
    "end_date": "2026-04-01T20:00:00Z",
    "status": "confirmed",
    "location": {
        "venue": "City Hall",
        "address_lines": ["1 Main St"],
        "locality": "Washington",
        "region": "DC",
        "postal_code": "20001"
    },
    "_links": {
        "self": {"href": "https://actionnetwork.org/api/v2/events/event-uuid-here"},
        "osdi:attendances": {"href": "..."},
        ...
    }
}
```

### Common ID extraction pattern

All Action Network resources follow the same pattern for IDs and links:

```python
# Get the resource UUID from any AN resource
uuid = resource["identifiers"][0].split(":")[-1]

# Get the full self-link URI (needed for cross-references like add_tagging)
uri = resource["_links"]["self"]["href"]
```

## Common Workflows

### HelpScout conversation -> Action Network person lookup and tagging

```python
from ccef_connections import HelpScoutConnector, ActionNetworkConnector

helpscout = HelpScoutConnector()
an = ActionNetworkConnector()

# 1. Read a HelpScout conversation and extract the customer email
conversations = helpscout.list_conversations(mailbox_id=12345, status="active")
conv = conversations[0]
full_conv = helpscout.get_conversation(conv["id"])
customer_email = full_conv["primaryCustomer"]["email"]

# 2. Find or create the person in Action Network (deduplicates by email)
person = an.create_person(
    email=customer_email,
    given_name=full_conv["primaryCustomer"].get("firstName"),
    family_name=full_conv["primaryCustomer"].get("lastName"),
    tags=["helpscout-contact"],        # Auto-tags on create
)

# 3. Get the person URI for further operations
person_uri = person["_links"]["self"]["href"]

# 4. Add additional tags
tag = an.create_tag("2026-outreach")   # Idempotent — returns existing if name matches
tag_id = tag["identifiers"][0].split(":")[-1]
an.add_tagging(tag_id, [person_uri])

# 5. Close the HelpScout conversation
helpscout.add_note(conv["id"], f"Synced to Action Network: {person_uri}")
helpscout.update_conversation_status(conv["id"], "closed")
```

### Bulk tag all people matching a filter

```python
an = ActionNetworkConnector()

# Find people in a specific zip code
people = an.list_people(filter="postal_code eq '20001'")

# Create/get the tag
tag = an.create_tag("dc-residents")
tag_id = tag["identifiers"][0].split(":")[-1]

# Tag each person (add_tagging expects full URI strings)
for person in people:
    person_uri = person["_links"]["self"]["href"]
    an.add_tagging(tag_id, [person_uri])
```

### HelpScout unsubscribe request -> Action Network unsubscribe

```python
from ccef_connections import HelpScoutConnector, ActionNetworkConnector

helpscout = HelpScoutConnector()
an = ActionNetworkConnector()

# 1. Read the HelpScout conversation requesting unsubscribe
conversations = helpscout.list_conversations(mailbox_id=12345, status="active")
for conv in conversations:
    threads = helpscout.list_threads(conv["id"])
    # (Your logic to detect unsubscribe intent in the thread body)

    # 2. Extract the customer email
    full_conv = helpscout.get_conversation(conv["id"])
    customer_email = full_conv["primaryCustomer"]["email"]

    # 3. Unsubscribe by email — no UUID lookup needed
    #    Scoped to the national org's API key; does not affect state/local groups
    an.unsubscribe_person_by_email(customer_email)

    # 4. Close the conversation
    helpscout.add_note(conv["id"], f"Unsubscribed {customer_email} from Action Network.")
    helpscout.update_conversation_status(conv["id"], "closed")
```

**Federation note:** The unsubscription is scoped to whichever group's API key is configured in `ACTION_NETWORK_API_KEY_PASSWORD`. In CCEF's federated structure, use the national org's API key to unsubscribe from the national list. State/local groups and their lists are unaffected. Subscribing someone *does* propagate up the network, but unsubscribing does *not* — this asymmetry is by design in Action Network.

## Testing

The library has 968 unit tests covering the connectors and core modules (every connector except `PTVConnector` and `SheetsWriterConnector`, which do not yet have dedicated test files).

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=ccef_connections

# Run tests for a specific connector
pytest tests/test_action_builder.py -v
pytest tests/test_action_network.py -v
pytest tests/test_asana.py -v
pytest tests/test_helpscout.py -v
pytest tests/test_zoom.py -v
pytest tests/test_airtable.py -v
pytest tests/test_bigquery.py -v
pytest tests/test_openai.py -v
pytest tests/test_sheets.py -v
pytest tests/test_roi_crm.py -v
pytest tests/test_github.py -v
pytest tests/test_hex.py -v
pytest tests/test_geocodio.py -v
pytest tests/test_email_connector.py -v

# Run core and config tests
pytest tests/test_core.py -v
pytest tests/test_config.py -v
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Format code
black src/ccef_connections tests/

# Sort imports
isort src/ccef_connections tests/

# Type checking
mypy src/ccef_connections

# Linting
ruff check src/ccef_connections
```

## Error Handling

The library provides specific exceptions for different error types:

```python
from ccef_connections import (
    CCEFConnectionError,      # Base exception
    CredentialError,          # Missing/invalid credentials
    ConnectionError,          # Connection failed (shadows builtins.ConnectionError)
    AuthenticationError,      # Auth failed
    RateLimitError,          # Rate limit exceeded
    ConfigurationError,       # Invalid configuration
    QueryError,              # Query failed
    WriteError,              # Write operation failed
)

try:
    connector = AirtableConnector()
    table = connector.get_table('appXXX', 'Table')
except CredentialError as e:
    print(f"Missing credentials: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
```

**Note:** `ccef_connections.ConnectionError` is a subclass of `CCEFConnectionError`, not the Python builtin `ConnectionError` (which inherits from `OSError`). If you need both, import with an alias that doesn't collide with the library's `CCEFConnectionError` base class:

```python
from ccef_connections import ConnectionError as CCEFConnError
# Now `ConnectionError` refers to the builtin, `CCEFConnError` to the library's.
```

Or simply catch the exported base class `CCEFConnectionError`, which sidesteps the shadowing entirely.

## Environment Variable Overrides

Configuration values from Google Sheets can be overridden with environment variables:

```bash
# Override config with environment variables
export CCEF_AIRTABLE_BASE_ID=appNewBaseId
export CCEF_OPENAI_MODEL=gpt-4-turbo
```

Format: `CCEF_{SECTION}_{KEY}` (all uppercase)

## License

MIT License - see LICENSE file for details.

## Support

For issues or questions:
- Open an issue in the repository
- Contact the CCEF tech team

## Version

Current version: 0.5.0
