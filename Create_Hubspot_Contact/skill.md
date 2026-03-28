# Skill: Create a New HubSpot Contact (Sales Qualified Lead)

## Overview

Use this skill whenever you need to create a brand new HubSpot contact with complete
profile details (including company name), mark the contact as a Sales Qualified Lead,
and immediately register the canonical ID in `cross_tool_mapping` so the
`HubSpotQualifiedSync` automation handles it correctly.

---

## Step 1 — Decide the contact's details

Gather **all** fields (required + optional) before writing any code. Every field in
the table below must be populated — do not leave any field as a placeholder.

If no contact details are provided, **generate them**. Follow the steps below in order.

---

### 1a — Pick a neighborhood first

Choose one of the recognised Austin neighbourhoods. The neighbourhood determines the
realistic street address you generate in step 1c.

| Neighbourhood        | ZIP   | Representative streets / landmarks                        |
|----------------------|-------|-----------------------------------------------------------|
| Downtown             | 78701 | Congress Ave, 6th St, Colorado St, Lavaca St              |
| East Austin / Mueller| 78723 | Airport Blvd, Manor Rd, Berkman Dr, Springdale Rd         |
| South Austin / Zilker| 78704 | S Lamar Blvd, Barton Springs Rd, S 1st St, Oltorf St      |
| Westlake / Tarrytown | 78746 | Westlake Dr, Bee Cave Rd, Walsh Tarlton Ln, Lake Austin Blvd |
| Round Rock / Cedar Park | 78665 | University Blvd, Gattis School Rd, Anderson Mill Rd    |
| North Loop / Crestview | 78756 | N Loop Blvd, Burnet Rd, Woodrow Ave, 49th St            |
| Domain / North Austin| 78758 | N MoPac Expy, Domain Dr, Braker Ln, Metric Blvd          |
| Steiner Ranch        | 78732 | Steiner Ranch Blvd, Quinlan Park Rd, River Hills Dr       |
| Bee Cave / Lakeway   | 78738 | Hwy 71, Bee Cave Pkwy, Ranch Rd 620, Lakeway Blvd         |

Generate a plausible building number (100–9999) and pick a street from the list above.

---

### 1b — Generate a unique name + email

**Rules (all three must pass before you finalise the contact):**

1. The `firstname` + `lastname` combination must not exist in the local database.
2. The `firstname` + `lastname` combination must not exist in HubSpot.
3. The `email` must not exist in the local database or in HubSpot.

**Check the local database:**

```python
import sqlite3, os
db = sqlite3.connect(os.path.join("sparkle_shine.db"))
db.row_factory = sqlite3.Row

# Name check — leads table
name_clash = db.execute(
    "SELECT id FROM leads WHERE lower(first_name)=lower(?) AND lower(last_name)=lower(?)",
    ("<FIRST>", "<LAST>"),
).fetchone()
# Name check — clients table
name_clash2 = db.execute(
    "SELECT id FROM clients WHERE lower(first_name)=lower(?) AND lower(last_name)=lower(?)",
    ("<FIRST>", "<LAST>"),
).fetchone()
# Email check
email_clash  = db.execute("SELECT id FROM leads   WHERE email=?", ("<email>",)).fetchone()
email_clash2 = db.execute("SELECT id FROM clients WHERE email=?", ("<email>",)).fetchone()

# If ANY of the above is not None, choose a different name / email and re-check.
```

**Check HubSpot (using the session built in step 2):**

```python
def _hubspot_name_exists(session, firstname, lastname):
    url = f"{_BASE_URL}/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "firstname", "operator": "EQ", "value": firstname},
                {"propertyName": "lastname",  "operator": "EQ", "value": lastname},
            ]
        }],
        "limit": 1,
    }
    resp = session.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json().get("total", 0) > 0

def _hubspot_email_exists(session, email):
    url = f"{_BASE_URL}/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [{
            "filters": [{"propertyName": "email", "operator": "EQ", "value": email}]
        }],
        "limit": 1,
    }
    resp = session.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json().get("total", 0) > 0
```

Call both helpers **before** updating the `CONTACT` dict. If either returns `True`,
choose a different name/email and re-check.

---

### 1c — All contact fields (every field is required)

| HubSpot property     | Notes                                                         |
|----------------------|---------------------------------------------------------------|
| `firstname`          | Contact first name — verified unique (step 1b)               |
| `lastname`           | Contact last name — verified unique (step 1b)                |
| `email`              | Unique email — verified unique (step 1b); format: `firstname.lastname@companydomain.com` |
| `company`            | **Always include — company name**                            |
| `lifecyclestage`     | Must be `"salesqualifiedlead"`                               |
| `phone`              | Texas number: `"(512) 555-XXXX"` or `"(737) 555-XXXX"`      |
| `jobtitle`           | Realistic title for the company type                         |
| `address`            | Real-looking street from the neighbourhood table in step 1a  |
| `city`               | Always `"Austin"` (or `"Round Rock"` / `"Cedar Park"` for that zone) |
| `state`              | Always `"TX"`                                                |
| `zip`                | ZIP from the neighbourhood table in step 1a                  |
| `hs_lead_status`     | Always `"IN_PROGRESS"`                                       |
| `client_type`        | `"commercial"` for business contacts, `"residential"` for homeowners |
| `service_frequency`  | `"weekly"`, `"biweekly"`, or `"monthly"` — match client type |
| `neighborhood`       | Exact neighbourhood string from the table in step 1a         |
| `lead_source_detail` | `"website_inquiry"`, `"referral"`, `"google_ads"`, or `"trade_show"` |

---

## Step 2 — Update the CONTACT dict in `create_hubspot_contact.py`

Edit **only** the `CONTACT` dict at the top of `create_hubspot_contact.py` in the
project root. Replace every value with the finalised contact details from step 1.
Do not leave any placeholder values.

```python
CONTACT = {
    "firstname":          "<FIRST>",
    "lastname":           "<LAST>",
    "email":              "<email@example.com>",
    "phone":              "<(512) 555-0100>",
    "company":            "<Company Name>",          # Always include
    "jobtitle":           "<Job Title>",
    "address":            "<Street Address>",
    "city":               "Austin",
    "state":              "TX",
    "zip":                "<ZIP>",
    "lifecyclestage":     "salesqualifiedlead",      # Always salesqualifiedlead
    "client_type":        "commercial",              # or "residential"
    "service_frequency":  "weekly",                  # weekly / biweekly / monthly
    "neighborhood":       "<Neighbourhood>",
    "lead_source_detail": "website_inquiry",         # or "referral", etc.
    "hs_lead_status":     "IN_PROGRESS",
}
```

---

## Step 3 — Run the script

```bash
cd sparkle-shine-poc
python3 create_hubspot_contact.py
```

### Expected output

```
============================================================
  Sparkle & Shine — Create SQL Contact in HubSpot
============================================================

Contact to create:
  firstname                  Nadia
  ...
  lifecyclestage             salesqualifiedlead

  Assigned canonical ID : SS-LEAD-0236

Pushing to HubSpot...

[OK] Contact created successfully.
  HubSpot ID     : 461642978022
  Email          : nadia.chen@vertexcoworking.example.com
  Lifecycle stage: salesqualifiedlead

Registering mapping: SS-LEAD-0236 → hubspot:461642978022
[OK] cross_tool_mapping updated.

  View in HubSpot: https://app.hubspot.com/contacts/*/contact/461642978022
```

---

## Step 4 — Verify

1. **HubSpot UI** — open the printed URL and confirm:
   - Lifecycle stage = `Sales Qualified Lead`
   - Company name is populated
   - All custom properties are present

2. **Database** — confirm the mapping was written:
   ```bash
   python3 -c "
   import sqlite3
   db = sqlite3.connect('sparkle_shine.db')
   db.row_factory = sqlite3.Row
   rows = db.execute(
       \"SELECT * FROM cross_tool_mapping ORDER BY synced_at DESC LIMIT 5\"
   ).fetchall()
   for r in rows:
       print(dict(r))
   "
   ```

3. **Automation** — the `HubSpotQualifiedSync` automation polls HubSpot every ~5 minutes
   for `salesqualifiedlead` contacts. Because `cross_tool_mapping` now has the hubspot
   row, the automation will skip the creation step and move straight to creating the
   Pipedrive person and deal on its next run.

---

## Key rules

| Rule | Why |
|---|---|
| Always set `lifecyclestage = "salesqualifiedlead"` | Any other value is ignored by `HubSpotQualifiedSync` |
| Always include `company` | Pipedrive deal title uses the company name |
| Always call `register_in_db` immediately after the API call succeeds | Prevents `HubSpotQualifiedSync` from minting a duplicate canonical ID on the next run |
| Always edit `create_hubspot_contact.py` — never create a new script per contact | One reusable script keeps the project root clean |
| Check name uniqueness in **both** the local DB and HubSpot before finalising | A name clash causes confusion in HubSpot's contact list and in cross-tool reports |
| Check email uniqueness in **both** the local DB and HubSpot before finalising | Email is HubSpot's dedup key — a clash silently merges into the existing record |
| Always populate **every** field in the `CONTACT` dict — no placeholders | Partial records break Pipedrive deal creation and the intelligence layer metrics |
| Generate the street address from the neighbourhood table in step 1a | Ensures addresses are geographically consistent with the assigned crew zone |
