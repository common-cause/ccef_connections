# CCEF Connections

A reusable Python library for Common Cause Education Fund data integrations. Provides unified connection management for Airtable, OpenAI, Google Sheets, BigQuery, HelpScout, Zoom, and Action Network with Civis credential compatibility.

## Features

- **Airtable Integration**: Automatic retry, batch operations, formula filtering
- **OpenAI/ChatGPT**: Langchain integration with structured outputs
- **Google Sheets**: Read-only configuration management
- **BigQuery**: Full read/write data warehouse operations
- **HelpScout**: Automated email processing — read conversations, reply, add notes, forward, close
- **Zoom**: Meeting and webinar attendee retrieval — participants, registrants, absentees
- **Action Network**: Full CRM access — people, tags, events, petitions, forms, fundraising, messages, and more
- **Unified Credentials**: `{CREDENTIAL_NAME}_PASSWORD` pattern for Civis compatibility
- **Automatic Retry**: Built-in exponential backoff for all APIs
- **Configuration as Code**: Manage settings via Google Sheets

## Installation

### Development Installation

```bash
# Clone or navigate to the repository
cd ccef-connections

# Install in editable mode with optional dependencies
pip install -e ".[dev,pandas]"
```

### From Another Project

```bash
# Install as editable from local path (adjust path to wherever the repo lives)
# Example — sibling directory:
pip install -e ../ccef-connections
# Example — full OneDrive path:
pip install -e "C:/Users/RobKerth/OneDrive - Common Cause Education Fund/Documents/AI Interpretation/ccef-connections"

# Or after publishing to PyPI
pip install ccef-connections
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
```

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

# Reply, add a note, and close the conversation
helpscout.reply_to_conversation(98765, "Thanks for reaching out!")
helpscout.add_note(98765, "Resolved via automation.")
helpscout.update_conversation_status(98765, 'closed')

# Forward a conversation
helpscout.forward_conversation(98765, to=["partner@example.com"])
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

This pattern is compatible with Civis Docker environments while also working seamlessly in local development with `.env` files.

### Retry Logic

All connectors include automatic retry with exponential backoff:

- **Airtable**: 5 retries, handles 5 req/sec rate limit
- **OpenAI**: 5 retries, handles 429 rate limit errors
- **Google APIs**: 5 retries, handles quota limits
- **HelpScout**: 5 retries, handles rate limits with auto token refresh on 401
- **Zoom**: 5 retries, handles rate limits with auto token refresh on 401
- **Action Network**: 5 retries, handles 429 rate limits (4 req/s)
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
- `update_record(base_id, table_name, record_id, fields)` - Update a record
- `batch_update(base_id, table_name, records)` - Update multiple records
- `create_record(base_id, table_name, fields)` - Create a new record

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

### BigQueryConnector

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
- `reply_to_conversation(conversation_id, text, customer=None, draft=False)` - Reply to a conversation
- `add_note(conversation_id, text)` - Add an internal note
- `update_conversation_status(conversation_id, status)` - Set status (active/pending/closed)
- `forward_conversation(conversation_id, to, note=None)` - Forward to external email

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

## Testing

The library has 540 unit tests covering all connectors and core modules.

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=ccef_connections

# Run tests for a specific connector
pytest tests/test_helpscout.py -v
pytest tests/test_zoom.py -v
pytest tests/test_action_network.py -v
pytest tests/test_airtable.py -v
pytest tests/test_bigquery.py -v
pytest tests/test_openai.py -v
pytest tests/test_sheets.py -v

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

**Note:** `ccef_connections.ConnectionError` is a subclass of `CCEFConnectionError`, not the Python builtin `ConnectionError` (which inherits from `OSError`). If you need both, import with an alias:

```python
from ccef_connections import ConnectionError as CCEFConnectionError
```

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

Current version: 0.1.0
