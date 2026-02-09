# CCEF Connections

A reusable Python library for Common Cause Education Fund data integrations. Provides unified connection management for Airtable, OpenAI, Google Sheets, BigQuery, HelpScout, and Zoom with Civis credential compatibility.

## Features

- **Airtable Integration**: Automatic retry, batch operations, formula filtering
- **OpenAI/ChatGPT**: Langchain integration with structured outputs
- **Google Sheets**: Read-only configuration management
- **BigQuery**: Full read/write data warehouse operations
- **HelpScout**: Automated email processing — read conversations, reply, add notes, forward, close
- **Zoom**: Meeting and webinar attendee retrieval — participants, registrants, absentees
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
# Install as editable from local path
pip install -e ../ccef-connections

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

This pattern is compatible with Civis Docker environments while also working seamlessly in local development with `.env` files.

### Retry Logic

All connectors include automatic retry with exponential backoff:

- **Airtable**: 5 retries, handles 5 req/sec rate limit
- **OpenAI**: 5 retries, handles 429 rate limit errors
- **Google APIs**: 5 retries, handles quota limits
- **HelpScout**: 5 retries, handles rate limits with auto token refresh on 401
- **Zoom**: 5 retries, handles rate limits with auto token refresh on 401
- **Transient errors**: Automatic retry for network failures

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

### ConfigManager

- `get_config()` - Get all configuration
- `get(section, key, default=None)` - Get specific value
- `refresh()` - Refresh from Google Sheets
- `clear_cache()` - Clear configuration cache

## Testing

The library has 452 unit tests covering all connectors and core modules.

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=ccef_connections

# Run tests for a specific connector
pytest tests/test_helpscout.py -v
pytest tests/test_zoom.py -v
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
    ConnectionError,          # Connection failed
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
