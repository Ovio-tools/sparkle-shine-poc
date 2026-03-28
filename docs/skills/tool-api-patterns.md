# Skill: Tool API Patterns

**When to read:** Before writing any code that calls a SaaS tool's API.

This doc covers auth setup, base URLs, request format, rate limiting, and error handling for all 8 tools in the Sparkle & Shine stack. Every pattern shown here is based on existing working code in the repo.

---

## Auth Overview

Two auth approaches exist in this codebase. Check `SIMULATION_AUDIT.md` (generated in Step 0 of the simulation plan) to confirm which one the rest of the codebase uses. Then match it.

**Approach A: `credentials.py` (root level)**
```python
from credentials import get_credential

token = get_credential("HUBSPOT_ACCESS_TOKEN")
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
})
```

**Approach B: `auth/` module**
```python
from auth.simple_clients import get_client

session = get_client("hubspot")  # returns a configured requests.Session
```

Use whichever the existing automations and intelligence layer use. Do not mix both in new code.

---

## HubSpot (Marketing & Contacts)

**Auth:** Private App Token (Bearer). Never expires.
**Base URL:** `https://api.hubapi.com`
**Header:**
```python
{"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
```

**Create a contact:**
```python
POST /crm/v3/objects/contacts
{
    "properties": {
        "email": "sarah.chen@example.com",
        "firstname": "Sarah",
        "lastname": "Chen",
        "phone": "(512) 555-0147",
        "address": "2401 Westlake Dr",
        "city": "Austin",
        "state": "TX",
        "zip": "78746",
        "lifecyclestage": "sales_qualified_lead",
        "client_type": "residential",
        "lead_source_detail": "referral",
        "service_interest": "biweekly_recurring",
        "hs_lead_status": "NEW"
    }
}
# Returns: {"id": "12345", "properties": {...}, "createdAt": "..."}
```

**Fetch a contact:**
```python
GET /crm/v3/objects/contacts/{contact_id}?properties=email,firstname,lastname,lifecyclestage,client_type
```

**Search contacts:**
```python
POST /crm/v3/objects/contacts/search
{
    "filterGroups": [{
        "filters": [{
            "propertyName": "lifecyclestage",
            "operator": "EQ",
            "value": "sales_qualified_lead"
        }]
    }],
    "properties": ["email", "firstname", "lastname", "lifecyclestage"]
}
```

**Batch create (up to 100):**
```python
POST /crm/v3/objects/contacts/batch/create
{"inputs": [{"properties": {...}}, {"properties": {...}}]}
```

**Update a contact:**
```python
PATCH /crm/v3/objects/contacts/{contact_id}
{"properties": {"lifecyclestage": "other", "hs_lead_status": "CHURNED"}}
```

**Rate limit:** 100 requests per 10 seconds. Use 0.12s delay between calls.
**Error codes:** 401 = token revoked. 409 = contact already exists (duplicate email). 429 = rate limited.

---

## Pipedrive (Sales Pipeline)

**Auth:** API token passed as query param or header.
**Base URL:** `https://api.pipedrive.com/v1`
**Header:**
```python
{"x-api-token": token, "Content-Type": "application/json"}
# OR as query param: ?api_token={token}
```

**Get deals:**
```python
GET /v1/deals?status=open&sort=update_time%20DESC&limit=100
# Returns: {"data": [{"id": 1, "title": "...", "value": 5000, "stage_id": 3, ...}]}
```

**Update a deal (advance stage or mark won):**
```python
PUT /v1/deals/{deal_id}
{"stage_id": 5}           # advance to a stage
# OR
{"status": "won", "won_time": "2026-03-27 14:00:00"}
# OR
{"status": "lost", "lost_reason": "Chose competitor"}
```

**Add custom field values to a deal:**
```python
PUT /v1/deals/{deal_id}
{
    "{custom_field_key}": "value"    # custom field keys are in config/tool_ids.json
}
```
Custom field keys look like hex hashes (e.g., `"abc123def456"`). Look them up in `config/tool_ids.json` under the Pipedrive section.

**Add an activity:**
```python
POST /v1/activities
{
    "subject": "Stage advanced to Negotiation",
    "deal_id": 123,
    "type": "task",
    "done": 1,
    "note": "Automated: deal progressed."
}
```

**Get a person:**
```python
GET /v1/persons/{person_id}
```

**Mark a person inactive:**
```python
PUT /v1/persons/{person_id}
{"active_flag": false}
```

**Rate limit:** 80 requests per 2 seconds (sandbox). Use 0.15s delay.
**Error codes:** 401 = bad token. 404 = deal/person not found. 429 = rate limited. Pipedrive returns `{"success": false, "error": "..."}` on failures.

**Important:** Pipedrive's API does not support `updatedSince` filtering. To find recently updated deals, sort by `update_time DESC` and pull the first page.

---

## Jobber (Operations & Scheduling)

**Auth:** OAuth 2.0. Access token refreshes every 60 minutes.
**Base URL:** `https://api.getjobber.com/api/graphql`
**Headers:**
```python
{
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "X-JOBBER-GRAPHQL-VERSION": "2026-03-10"
}
```

**Jobber uses GraphQL, not REST.** All mutations and queries go to the same endpoint.

**Create a client:**
```python
mutation {
    clientCreate(input: {
        firstName: "Sarah"
        lastName: "Chen"
        email: "sarah.chen@example.com"
        phone: "(512) 555-0147"
        billingAddress: {
            street: "2401 Westlake Dr"
            city: "Austin"
            state: "TX"
            postalCode: "78746"
        }
        note: "SS-ID: SS-CLIENT-0312 | Service: biweekly_recurring | Crew: Crew A"
    }) {
        client { id firstName lastName }
    }
}
```

**Create a job:**
```python
mutation {
    jobCreate(input: {
        clientId: "{jobber_client_id}"
        title: "Biweekly Residential Clean"
        startAt: "2026-04-01T09:00:00-05:00"
        endAt: "2026-04-01T11:00:00-05:00"
        lineItems: [{
            name: "Biweekly Residential Clean"
            unitPrice: 150.00
            quantity: 1
        }]
    }) {
        job { id title startAt }
    }
}
```

**Query clients:**
```python
query {
    clients(first: 50) {
        nodes { id firstName lastName email updatedAt }
        pageInfo { hasNextPage endCursor }
    }
}
```

**Query jobs:**
```python
query {
    jobs(first: 50, filter: { updatedAtOrAfter: "2026-03-26T00:00:00Z" }) {
        nodes { id title startAt endAt status client { id } }
    }
}
```

**Token refresh:** handled by `auth/jobber_auth.py`. Tokens are stored in `.jobber_tokens.json`. If you get a 401, call the refresh flow before retrying.

**Rate limit:** 10 requests per second. Use 0.15s delay.
**Error format:** GraphQL errors appear in `{"errors": [{"message": "..."}]}` alongside partial `{"data": {...}}`.

---

## QuickBooks Online (Finance)

**Auth:** OAuth 2.0. Access token refreshes every 60 minutes. Refresh token valid for 100 days.
**Base URL (sandbox):** `https://sandbox-quickbooks.api.intuit.com/v3/company/{COMPANY_ID}`
**Headers:**
```python
{
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}
```

**Create an invoice:**
```python
POST /v3/company/{COMPANY_ID}/invoice
{
    "CustomerRef": {"value": "{qbo_customer_id}"},
    "DueDate": "2026-04-15",
    "Line": [{
        "Amount": 150.00,
        "DetailType": "SalesItemLineDetail",
        "SalesItemLineDetail": {
            "ItemRef": {"value": "{service_item_id}"},
            "Qty": 1,
            "UnitPrice": 150.00
        },
        "Description": "Biweekly Residential Clean - 2026-03-27"
    }],
    "PrivateNote": "SS-ID: SS-JOB-8201 | Crew A"
}
```

**Create a payment:**
```python
POST /v3/company/{COMPANY_ID}/payment
{
    "CustomerRef": {"value": "{qbo_customer_id}"},
    "TotalAmt": 150.00,
    "Line": [{
        "Amount": 150.00,
        "LinkedTxn": [{
            "TxnId": "{qbo_invoice_id}",
            "TxnType": "Invoice"
        }]
    }]
}
```

**Query invoices:**
```python
GET /v3/company/{COMPANY_ID}/query?query=SELECT * FROM Invoice WHERE MetaData.LastUpdatedTime > '2026-03-26'
```

**Create a customer:**
```python
POST /v3/company/{COMPANY_ID}/customer
{
    "DisplayName": "Sarah Chen",
    "PrimaryEmailAddr": {"Address": "sarah.chen@example.com"},
    "PrimaryPhone": {"FreeFormNumber": "(512) 555-0147"},
    "BillAddr": {
        "Line1": "2401 Westlake Dr",
        "City": "Austin",
        "CountrySubDivisionCode": "TX",
        "PostalCode": "78746"
    },
    "Notes": "SS-ID: SS-CLIENT-0312"
}
```

**Token refresh:** handled by `auth/quickbooks_auth.py`. Tokens stored in `.quickbooks_tokens.json`.

**Rate limit:** 500 requests per minute. Use 0.15s delay.
**Error codes:** 401 = token expired (refresh and retry). 400 = validation error (check `{"Fault": {"Error": [...]}}` for details). 6240 = duplicate (entity already exists).

---

## Asana (Back-Office Tasks)

**Auth:** Personal Access Token (Bearer). Never expires.
**Base URL:** `https://app.asana.com/api/1.0`
**Headers:**
```python
{"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
```

**Create a task:**
```python
POST /api/1.0/tasks
{
    "data": {
        "workspace": "{workspace_gid}",
        "projects": ["{project_gid}"],
        "name": "Retention follow-up: Sarah Chen cancelled",
        "notes": "Reason: Switching to competitor. LTV: $2,400.\nSS-ID: SS-CLIENT-0312",
        "due_on": "2026-03-31",
        "assignee": "{assignee_gid}"
    }
}
```

**Complete a task:**
```python
PUT /api/1.0/tasks/{task_gid}
{"data": {"completed": true}}
```

**Get tasks for a project:**
```python
GET /api/1.0/projects/{project_gid}/tasks?opt_fields=name,completed,due_on,assignee.name,memberships.section.name&completed_since=now
```
The `completed_since=now` filter returns only incomplete tasks.

**Project GIDs** are stored in `config/tool_ids.json` under the Asana section. The 4 projects:
- Sales Pipeline Tasks
- Marketing Calendar
- Admin & Operations
- Client Success

**Assignee GIDs** are also in `config/tool_ids.json`.

**Rate limit:** 150 requests per minute (free plan). Use 0.45s delay.
**Error codes:** 401 = bad PAT. 403 = no access to project. 429 = rate limited (check `Retry-After` header).

---

## Mailchimp (Email Marketing)

**Auth:** API key via Basic Auth. Never expires.
**Base URL:** `https://{server_prefix}.api.mailchimp.com/3.0`
The server prefix (e.g., `us6`) comes from `MAILCHIMP_SERVER_PREFIX` in `.env`.
**Headers:**
```python
# Use Basic Auth with "anystring" as username and API key as password
import base64
auth = base64.b64encode(f"anystring:{api_key}".encode()).decode()
{"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
```

**Add/update a subscriber:**
```python
PUT /3.0/lists/{audience_id}/members/{subscriber_hash}
# subscriber_hash = MD5 of lowercase email
{
    "email_address": "sarah.chen@example.com",
    "status": "subscribed",
    "merge_fields": {
        "FNAME": "Sarah",
        "LNAME": "Chen",
        "CLIENT_TYPE": "residential"
    },
    "tags": ["residential", "referral", "biweekly"]
}
```
Use PUT (not POST) for idempotent add-or-update.

**Unsubscribe:**
```python
PATCH /3.0/lists/{audience_id}/members/{subscriber_hash}
{"status": "unsubscribed"}
```

**Add a tag:**
```python
POST /3.0/lists/{audience_id}/members/{subscriber_hash}/tags
{"tags": [{"name": "churned", "status": "active"}]}
```

**Rate limit:** 10 requests per second. Use 0.15s delay.
**Error codes:** 401 = bad API key. 400 = validation (check `{"detail": "..."}` for the specific error).

**Subscriber hash:** `hashlib.md5(email.lower().encode()).hexdigest()`

---

## Slack (Internal Comms)

**Auth:** Bot User OAuth Token (Bearer). Never expires.
**Base URL:** `https://slack.com/api`
**Headers:**
```python
{"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"}
```

**Post a message:**
```python
POST /api/chat.postMessage
{
    "channel": "{channel_id}",
    "text": "Fallback text for notifications",
    "blocks": [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": ":warning: Automation Issue"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*What happened:* Connection to QuickBooks expired."}
        }
    ]
}
```

**Resolve channel name to ID:**
```python
GET /api/conversations.list?types=public_channel&limit=200
# Find channel by name in the response. Cache the result.
```

**Create a channel:**
```python
POST /api/conversations.create
{"name": "automation-failure", "is_private": false}
```

**Slack mrkdwn formatting:**
- Bold: `*bold*` (single asterisk, not double)
- Links: `<https://example.com|Display Text>`
- Code: `` `inline code` ``
- Emoji: `:warning:`, `:rotating_light:`, `:sunrise:`, `:bar_chart:`, `:mag:`

**Rate limit:** 1 message per second per channel. Use 1.1s delay.
**Bot scopes required:** `chat:write`, `channels:read`, `channels:join`, `channels:history`, `im:write`, `conversations:create` (for creating #automation-failure).

---

## Google Workspace (Drive, Docs, Sheets, Calendar, Gmail)

**Auth:** OAuth 2.0 Desktop app flow. Token refreshes every 60 minutes. Refresh token in `token.json`.
**Warning:** If the Google Cloud app is still in "Testing" mode, tokens expire after 7 days. Publishing the app (even without verification) fixes this.

Google APIs are accessed via the `google-api-python-client` library, not raw HTTP. Auth handled by `auth/google_auth.py`.

```python
from googleapiclient.discovery import build

# Drive
drive = build('drive', 'v3', credentials=creds)
files = drive.files().list(q="'folder_id' in parents", fields="files(id,name,modifiedTime)").execute()

# Calendar
calendar = build('calendar', 'v3', credentials=creds)
events = calendar.events().list(calendarId='primary', timeMin=start, timeMax=end).execute()

# Gmail (metadata only)
gmail = build('gmail', 'v1', credentials=creds)
messages = gmail.users().messages().list(userId='me', q=f'after:{epoch}').execute()
```

The simulation engine likely won't create new Google documents or calendar events (those are seeded data from Phase 1). But the intelligence syncers read from Google, so the auth pattern matters.

---

## Error Handling Pattern (All Tools)

Every API call should follow this pattern:

```python
import requests
from time import sleep

def call_tool_api(method, url, session, payload=None, max_retries=3):
    """Generic pattern for tool API calls with retry."""
    for attempt in range(max_retries):
        try:
            if method == "GET":
                resp = session.get(url, timeout=30)
            elif method == "POST":
                resp = session.post(url, json=payload, timeout=30)
            elif method == "PUT":
                resp = session.put(url, json=payload, timeout=30)
            elif method == "PATCH":
                resp = session.patch(url, json=payload, timeout=30)

            if resp.status_code in (200, 201):
                return resp.json()

            if resp.status_code == 401:
                # Token expired. Do NOT retry. Raise for upstream handling.
                raise TokenExpiredError(tool_name, resp.text[:200])

            if resp.status_code == 429:
                # Rate limited. Respect Retry-After if present.
                retry_after = int(resp.headers.get("Retry-After", 5))
                sleep(retry_after)
                continue

            if resp.status_code >= 500:
                # Server error. Wait and retry.
                sleep(2 ** attempt)
                continue

            # Client error (400, 403, 404). Do not retry.
            raise ToolAPIError(f"{resp.status_code}: {resp.text[:300]}")

        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
                continue
            raise

    raise ToolUnavailableError(tool_name, "Max retries exhausted")
```

**The throttler** in `seeding/utils/throttler.py` has pre-configured rate limits for all tools. Import and use the appropriate `Throttler` instance before each API call:

```python
from seeding.utils.throttler import HUBSPOT as hubspot_throttler

hubspot_throttler.wait()  # blocks until safe to call
response = session.post(url, json=payload, timeout=30)
```
