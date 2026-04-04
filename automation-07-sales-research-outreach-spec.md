# Automation #7: Sales Research & Outreach Agent Chain

**Status:** Design Complete, Pending Build
**Last updated:** April 2, 2026
**Dependencies:** HubSpot, Jobber, Gmail, Slack, Anthropic API (Claude Opus for Agents 1 & 3, Claude Sonnet for Agent 2)
**Model rationale:** Client-facing output requires Opus-quality writing and research. Agent 2 (internal data formatting) stays on Sonnet.

---

## 1. Purpose

When a new contact is created in HubSpot, automatically research the lead, find similar completed jobs in Jobber, and synthesize a personalized Gmail draft for Maria. Post a summary with draft link to #sales in Slack.

**Business value:** Speed-to-lead. A personalized, context-rich draft is ready for review within 60 seconds of a new inquiry landing in HubSpot. Maria reviews, tweaks if needed, and sends. No manual research, no copy-paste, no forgotten follow-ups.

---

## 2. Trigger

**Type:** Polling (consistent with Automations #1-6)
**Mechanism:** Automation runner checks HubSpot for new contacts on each 5-minute poll cycle
**Detection:** Compare HubSpot contact `createdate` against last poll timestamp in automation runner's watermark system
**Filter:** Only fire for contacts with `lifecyclestage = lead` or `lifecyclestage = subscriber`. Skip contacts created by other automations (check for `source = automation` tag or equivalent).

### Trigger Data Extracted from HubSpot

| Field | HubSpot Property | Required | Used By |
|-------|-----------------|----------|---------|
| First name | `firstname` | Yes | Agent 3 |
| Last name | `lastname` | Yes | Agents 1, 3 |
| Email | `email` | Yes | Agent 3 (Gmail draft) |
| Company name | `company` | No | Agents 1, 2, 3 |
| Address | `address` | No | Agents 1, 2 |
| City | `city` | No | Agents 1, 2 |
| Contact type | `contact_type` (custom) | No | Template selection |
| Lead source | `hs_analytics_source` + `hs_analytics_source_data_1` | No | Agent 3 |
| Service interest | `service_interest` (custom) | No | Agent 2 |

**If `contact_type` is missing or empty:** Template selection defaults to hybrid (see Section 7).

---

## 3. Agent Architecture

```
[HubSpot: new contact detected]
            |
            v
    Extract contact record
            |
      +-----+-----+
      |             |
      v             v
   Agent 1       Agent 2
  (Research)   (Similar Jobs)
      |             |
      +-----+-----+
            |
            v
         Agent 3
       (Synthesis)
            |
      +-----+-----+
      |             |
      v             v
  Gmail API      Slack API
 (create draft)  (post to #sales)
```

**Agents 1 and 2 run in parallel** using `concurrent.futures.ThreadPoolExecutor` (or `asyncio.gather()` if the codebase moves to async). Agent 3 fires only after both complete.

**Estimated total latency:** 30-90 seconds (dominated by Agent 1's web search calls; Opus is slower than Sonnet)

**Estimated cost per lead:** $0.15-0.30 (two Opus calls for Agents 1 & 3 with web search + one Sonnet call for Agent 2 formatting). At 50 leads/month, that's $7.50-15.00/month.

---

## 4. Agent 1: Lead Research

**Model:** claude-opus-4-6 with web search tool enabled
**Purpose:** Gather publicly available context about the lead to personalize outreach
**Input:** Contact name, company name (if present), address/city (if present), contact type (if present)

### System Prompt

```
You are a research assistant for Sparkle & Shine Cleaning Co., a residential
and commercial cleaning company in Austin, TX. Your job is to find publicly
available information about a new lead that would help personalize a sales
outreach email.

SEARCH STRATEGY:
- For commercial leads (or when a company name is provided): Search for the
  business name + city. Look for business type, services offered, approximate
  size, years in operation, number of employees, and online review sentiment.
- For residential leads (no company name): Search for the neighborhood or
  area characteristics. Look for property types common in the area, community
  features, and any relevant local context.
- If an address is provided, search for the neighborhood name and
  characteristics.

INFORMATION YOU MAY INCLUDE:
- Business category and services (commercial)
- Approximate business size or employee count (commercial)
- Years in operation (commercial)
- Online review sentiment summary (commercial)
- Neighborhood name and characteristics (residential)
- Property types common in the area (residential)
- Community features relevant to cleaning needs (residential)

INFORMATION YOU MUST NEVER INCLUDE:
- Personal social media content or posts
- Family information or household composition
- Property value, sale price, or financial details
- Political or religious affiliations
- Health information
- Any detail that would make the recipient uncomfortable knowing you found it

CONFIDENCE SCORING:
- "high": Found 3+ relevant, usable details
- "medium": Found 1-2 relevant details
- "low": Found little to nothing useful

OUTPUT FORMAT:
Respond with ONLY a JSON object, no preamble, no markdown backticks:
{
  "contact_type_inferred": "residential" | "commercial" | "unknown",
  "business_type": "string or null",
  "business_details": "string or null",
  "neighborhood_context": "string or null",
  "notable_details": "string or null",
  "research_confidence": "high" | "medium" | "low",
  "raw_summary": "2-3 sentence natural language summary of findings"
}
```

### User Prompt Template

```
Research this new lead for Sparkle & Shine Cleaning Co.:

Name: {first_name} {last_name}
Company: {company_name or "Not provided"}
Address: {address or "Not provided"}
City: {city or "Austin, TX"}
Contact type: {contact_type or "Not specified"}

Find publicly available information that would help us personalize our
outreach. Follow the search strategy and output format in your instructions.
```

### Error Handling

- If the API call fails or times out (30s timeout): Set `research_confidence` to `"low"` and `raw_summary` to `"Research unavailable"`. Continue to Agent 3.
- If the response is not valid JSON: Attempt to extract JSON from the response text. If that fails, use the fallback above.

---

## 5. Agent 2: Similar Job Matching

**Purpose:** Find 1-2 completed Jobber jobs most similar to the new lead
**Two-step process:** SQL query (no LLM needed) followed by a formatting call (Sonnet, no web search)

### Step 1: SQL Similarity Query

The query ranks completed jobs by a weighted similarity score across three dimensions.

```sql
SELECT
    j.id AS job_id,
    j.client_id,
    c.name AS client_name,
    j.service_type,
    j.total_amount,
    j.completed_date,
    j.neighborhood,
    j.property_type,
    j.duration_minutes,
    r.rating,
    r.review_text,
    (
        -- Service type alignment (strongest signal, 50 points)
        CASE
            WHEN j.service_type = %s THEN 50
            WHEN j.service_type IN ('standard_clean', 'deep_clean', 'recurring_biweekly')
                AND %s = 'residential' THEN 30
            WHEN j.service_type IN ('commercial_nightly', 'commercial_deep', 'commercial_project')
                AND %s = 'commercial' THEN 30
            ELSE 10
        END
        +
        -- Geographic proximity (30 points)
        CASE
            WHEN j.neighborhood = %s THEN 30
            WHEN j.crew_zone = %s THEN 20
            ELSE 5
        END
        +
        -- Recency bonus (20 points)
        CASE
            WHEN j.completed_date >= CURRENT_DATE - INTERVAL '30 days' THEN 20
            WHEN j.completed_date >= CURRENT_DATE - INTERVAL '90 days' THEN 15
            WHEN j.completed_date >= CURRENT_DATE - INTERVAL '180 days' THEN 10
            ELSE 5
        END
    ) AS similarity_score
FROM jobs j
JOIN clients c ON j.client_id = c.id
LEFT JOIN reviews r ON j.id = r.job_id AND r.rating >= 4
WHERE j.status = 'completed'
ORDER BY similarity_score DESC, j.completed_date DESC
LIMIT 2;
```

**Parameter tuple (psycopg2 `%s` positional):**
```python
params = (requested_service, contact_type, contact_type, lead_neighborhood, lead_crew_zone)
```

**Parameter sources:**
- `requested_service` -- from HubSpot `service_interest` custom property (may be null)
- `contact_type` -- from HubSpot `contact_type` custom property (may be null; if null, skip the type-based scoring tier and use 10 points flat). Passed twice because `%s` is positional.
- `lead_neighborhood` -- derived from address via neighborhood lookup table
- `lead_crew_zone` -- derived from neighborhood-to-zone mapping in `config/business.py`

**If no address is provided:** Geographic scoring defaults to 5 points for all jobs (no proximity boost).

### Step 2: Format Matches (claude-sonnet-4-6, No Web Search)

```
You are a research assistant for Sparkle & Shine Cleaning Co.

Given these completed job records, write a brief natural language description
of each that could be referenced in a sales email. Focus on: what the service
was, where it was (neighborhood only, never the client's name or address),
how often it recurs, and the price point.

RULES:
- Never include the matched client's name (privacy)
- Never include exact addresses
- Use neighborhood names only
- Keep each description to 1-2 sentences

OUTPUT FORMAT:
Respond with ONLY a JSON object, no preamble, no markdown backticks:
{
  "matches": [
    {
      "job_id": "string",
      "description": "string",
      "service_type": "string",
      "price": number,
      "frequency": "one-time" | "weekly" | "biweekly" | "monthly" | "nightly" | "custom",
      "neighborhood": "string"
    }
  ],
  "match_confidence": "high" | "medium" | "low",
  "estimated_annual_value": number | null
}
```

**Match confidence logic:**
- `"high"`: Top match has similarity score >= 80
- `"medium"`: Top match has similarity score 50-79
- `"low"`: Top match has similarity score < 50 or no results returned

**Estimated annual value calculation:**
- Recurring services: `price * frequency_multiplier` (weekly=52, biweekly=26, monthly=12, nightly=260)
- One-time services: Use the job total as-is (no annualization)
- If match confidence is `"low"`: Set to `null` (don't estimate)

### Error Handling

- If the SQL query returns no results: Set `match_confidence` to `"low"`, return empty matches array. Agent 3 will use a template that doesn't reference similar jobs.
- If the formatting Sonnet call fails: Use raw SQL data directly and format inline in Agent 3's prompt.

---

## 6. Agent 3: Email Synthesis

**Model:** claude-opus-4-6 (no web search needed)
**Purpose:** Generate a personalized email draft using outputs from Agents 1 and 2
**Input:** Contact record from HubSpot + Agent 1 output + Agent 2 output

### Template Selection Logic

```python
def select_template(contact_type, research_confidence, match_confidence):
    """
    Determine which template set and variant to use.

    Template sets: residential, commercial, hybrid
    Variants: A (similar work angle), B (expertise/needs angle), C (short and direct)
    """

    # Step 1: Determine template set
    if contact_type == "residential":
        template_set = "residential"
    elif contact_type == "commercial":
        template_set = "commercial"
    else:
        # Missing, ambiguous, or unknown contact type
        template_set = "hybrid"

    # Step 2: Select variant based on confidence scores
    if match_confidence == "high":
        variant = "A"  # Lead with similar work (strong match to reference)
    elif research_confidence in ("high", "medium") and match_confidence == "medium":
        variant = "B"  # Lead with expertise/needs (good research, decent match)
    else:
        variant = "C"  # Short and direct (thin data, don't stretch)

    # Step 3: Randomization override for variety
    # When both confidences are "high", randomly pick A or B (50/50)
    # to prevent all strong-match leads getting identical structures
    if research_confidence == "high" and match_confidence == "high":
        variant = random.choice(["A", "B"])

    return template_set, variant
```

### Lead Source Mapping

```python
LEAD_SOURCE_OPENERS = {
    "ORGANIC_SEARCH": "Thanks for finding us online",
    "PAID_SEARCH": "Thanks for reaching out through our ad",
    "REFERRAL": "Thanks for getting in touch -- {referrer_name} mentioned you might be looking for a cleaning service",
    "DIRECT_TRAFFIC": "Thanks for visiting our website and reaching out",
    "EMAIL_MARKETING": "Thanks for reaching out after our recent email",
    "SOCIAL_MEDIA": "Thanks for connecting with us",
    "OFFLINE": "Thanks for getting in touch",
    "OTHER": "Thanks for reaching out",
    None: "Thanks for reaching out"
}
```

If the lead source is `REFERRAL` and a referrer name is available in HubSpot (via `hs_analytics_source_data_1` or a custom property), include it. Otherwise, drop the referrer clause and use a generic referral opener: "Thanks for getting in touch -- we heard you were referred to us."

### System Prompt

```
You are writing a short sales outreach email on behalf of Maria Gonzalez,
owner of Sparkle & Shine Cleaning Co. in Austin, TX. Maria's tone is warm,
professional, and direct. She sounds like a real person, not a marketing
department.

You will receive:
- The lead's contact info
- A lead source opener line (use this as the first sentence after the greeting)
- Research findings about the lead (from a research agent)
- Similar completed job matches (from a job matching agent)
- A template variant instruction (A, B, or C)
- A template set instruction (residential, commercial, or hybrid)

TEMPLATE SET GUIDANCE:

Residential:
- Use the lead's first name
- Warm, conversational tone
- References to "your home" or "homes like yours"
- Sign off with just "Maria" and title below

Commercial:
- Use honorific + last name if a title is known (e.g., "Dr. Rivera"),
  otherwise "Mr./Ms. {Last Name}"
- Professional tone, still personable
- References to "your facility," "your practice," "your office"
- Sign off with full name, title, email, and phone

Hybrid (use when contact type is unknown):
- Use the lead's first name (safer than guessing an honorific)
- Professional but approachable tone (split the difference)
- Use neutral language: "your space" or "your property" instead of
  "your home" or "your facility"
- Do not assume residential or commercial; let the lead self-identify
- Sign off with full name, title, email, and phone

VARIANT GUIDANCE:

Variant A (Similar Work Angle):
- Open with the lead source opener
- Second paragraph: reference the similar job match. Mention the
  neighborhood and service type. Never name the matched client.
- Third paragraph: ask a qualifying question OR invite a call
- Include the scheduling link

Variant B (Expertise/Needs Angle):
- Open with the lead source opener
- Second paragraph: demonstrate understanding of the lead's likely
  needs, using research findings. For commercial, reference
  industry-specific cleaning concerns. For residential, reference
  neighborhood or lifestyle context.
- Third paragraph: mention Sparkle & Shine's presence in the area
  and relevant experience
- Fourth paragraph: invite a call with scheduling link

Variant C (Short and Direct):
- Open with the lead source opener
- One short paragraph combining: area familiarity + similar work
  reference + approximate pricing (if match data supports it)
- One sentence with scheduling link
- Keep the entire email under 100 words (excluding signature)

RULES:
- Never mention that research was conducted on the lead
- Never reference AI, automation, or agents
- Never use the matched client's name
- Never repeat the lead's first name more than once in the body
- Keep the email under 150 words for variants A and B, under 100
  for variant C (excluding signature)
- Always end with the scheduling link placeholder: [scheduling link]
- The email should read as if Maria wrote it herself in under 2 minutes

OUTPUT FORMAT:
Respond with ONLY a JSON object, no preamble, no markdown backticks:
{
  "subject": "string",
  "body": "string",
  "template_set_used": "residential" | "commercial" | "hybrid",
  "variant_used": "A" | "B" | "C",
  "word_count": number
}
```

### User Prompt Template

```
Write a sales outreach email with the following inputs:

LEAD INFO:
- Name: {first_name} {last_name}
- Email: {email}
- Company: {company_name or "Not provided"}
- Contact type: {contact_type or "Not specified"}

TEMPLATE INSTRUCTIONS:
- Template set: {template_set}
- Variant: {variant}
- Lead source opener: "{lead_source_opener}"

RESEARCH FINDINGS (from research agent):
{agent_1_output.raw_summary}
Research confidence: {agent_1_output.research_confidence}

SIMILAR JOB MATCHES (from job matching agent):
{formatted_matches or "No strong matches found."}
Match confidence: {agent_2_output.match_confidence}

SCHEDULING LINK PLACEHOLDER: [scheduling link]

MARIA'S SIGNATURE:
{signature based on template_set -- see below}
```

### Signature Blocks

**Residential:**
```
Maria
Sparkle & Shine Cleaning Co.
(512) 555-0147
```

**Commercial and Hybrid:**
```
Maria Gonzalez
Owner, Sparkle & Shine Cleaning Co.
(512) 555-0147
info@sparkleshineaustin.com
```

### Error Handling

- If Agent 3 returns invalid JSON: Attempt extraction. If that fails, log the error, skip the Gmail draft, and post a Slack alert to #sales with the raw contact info and a note that auto-drafting failed.
- If the generated email exceeds word count limits: Log a warning but use the draft anyway (don't retry). Maria can trim.

---

## 7. Hybrid Templates: Design Notes

The hybrid set exists for leads where `contact_type` is missing, empty, or set to an unrecognized value. This is more common than it might seem. Web form submissions don't always capture intent. Someone from a property management company might fill out a "residential" form, or a homeowner might click on a commercial ad by mistake.

**Hybrid design principles:**
- Use the lead's first name (guessing an honorific for an unknown contact type risks getting it wrong)
- Replace property-specific language with neutral terms: "your space," "your property," "keeping things clean," "your cleaning needs"
- Don't reference "home" or "office" or "facility" -- let the lead mentally fill in the blank
- Mention that Sparkle & Shine serves both residential and commercial clients (subtle self-identification prompt)
- Use the commercial-style signature (more professional, works for both audiences)

**Example hybrid variant A opening:**

> "Hi Alex, thanks for reaching out. We clean a number of properties in the
> [neighborhood] area already, including a space similar in scope to yours
> that we service on a biweekly basis."

The word "properties" works for houses, offices, and everything in between. The word "space" avoids assuming what kind of property it is.

---

## 8. Gmail Draft Creation

**API:** Gmail API v1 (`users.drafts.create`)
**Auth:** Google Workspace OAuth via `auth.get_client("google")` (requires `https://www.googleapis.com/auth/gmail.compose` scope)

### Implementation

```python
import base64
from email.mime.text import MIMEText
from auth import get_client

def create_gmail_draft(to_email, subject, body_text):
    """
    Create a Gmail draft and return the draft ID + deep link.

    Uses auth.get_client("google") for the authenticated Gmail service.

    Args:
        to_email: Recipient email address
        subject: Email subject line
        body_text: Plain text email body

    Returns:
        dict: {"draft_id": str, "gmail_link": str}
    """
    service = get_client("google", service_name="gmail", version="v1")

    message = MIMEText(body_text)
    message["to"] = to_email
    message["subject"] = subject
    message["from"] = "maria@sparkleshineaustin.com"

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

    draft = service.users().drafts().create(
        userId="me",
        body={"message": {"raw": raw}}
    ).execute()

    draft_id = draft["id"]
    # Gmail deep link format for Workspace accounts
    gmail_link = f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}"

    return {"draft_id": draft_id, "gmail_link": gmail_link}
```

### Gmail Scope Requirement

The existing Google OAuth setup must include the `gmail.compose` scope. If it's not already in the authorized scopes, this needs to be added before this automation can run. Check `auth/google_auth.py` for the current scope list. Adding a new scope will invalidate the existing `token.json` and require a one-time re-auth flow.

### Deep Link Testing Note

The `u/0` index in the Gmail URL assumes the first logged-in Google account. For the POC (single Workspace account on Railway), this works. For a production deployment with multiple accounts, the index may need to be dynamic. **Test this early** -- if the deep link doesn't open the draft in compose view, the fallback is linking to the drafts folder: `https://mail.google.com/mail/u/0/#drafts`

---

## 9. Slack Notification

**Channel:** #sales
**Format:** Slack Block Kit (consistent with existing automation notifications)

### Message Structure

```json
{
  "channel": "sales",
  "blocks": [
    {
      "type": "header",
      "text": {
        "type": "plain_text",
        "text": "New Lead Draft Ready"
      }
    },
    {
      "type": "section",
      "fields": [
        {
          "type": "mrkdwn",
          "text": "*Contact:*\n{first_name} {last_name}"
        },
        {
          "type": "mrkdwn",
          "text": "*Type:*\n{contact_type_display}"
        },
        {
          "type": "mrkdwn",
          "text": "*Source:*\n{lead_source_display}"
        },
        {
          "type": "mrkdwn",
          "text": "*Research Confidence:*\n{research_confidence_emoji} {research_confidence}"
        },
        {
          "type": "mrkdwn",
          "text": "*Best Match:*\n{match_summary}"
        },
        {
          "type": "mrkdwn",
          "text": "*Est. Deal Value:*\n{estimated_annual_value_display}"
        }
      ]
    },
    {
      "type": "actions",
      "elements": [
        {
          "type": "button",
          "text": {
            "type": "plain_text",
            "text": "Open Gmail Draft"
          },
          "url": "{gmail_link}",
          "style": "primary"
        },
        {
          "type": "button",
          "text": {
            "type": "plain_text",
            "text": "View in HubSpot"
          },
          "url": "{hubspot_contact_url}"
        }
      ]
    }
  ]
}
```

### Display Formatting

| Field | Formatting Rule |
|-------|----------------|
| `contact_type_display` | "Residential," "Commercial," or "Unknown (hybrid draft)" |
| `lead_source_display` | Human-readable: "Google Ads," "Referral," "Website," "Email Campaign," etc. |
| `research_confidence_emoji` | High = green circle, Medium = yellow circle, Low = red circle |
| `match_summary` | E.g., "Biweekly standard clean, Westlake ($150/visit)" or "No strong match" |
| `estimated_annual_value_display` | E.g., "~$3,900/year" or "N/A" if null |
| `hubspot_contact_url` | `https://app.hubspot.com/contacts/{portal_id}/contact/{contact_id}` |

---

## 10. End-to-End Data Flow Summary

```
1. Automation runner polls HubSpot via auth.get_client("hubspot") (every 5 min)
2. Detects new contact (createdate > last watermark)
3. Extracts contact record (name, company, address, type, source, email)
4. Determines lead_neighborhood and lead_crew_zone from address
5. Maps lead_source to opener line via LEAD_SOURCE_OPENERS dict

   --- PARALLEL START ---

6a. Agent 1: Anthropic API call (Opus + web search)
    Input: name, company, address, contact type
    Output: research JSON (business_type, neighborhood_context,
            research_confidence, raw_summary)

6b. Agent 2 Step 1: PostgreSQL similarity query against jobs table
    via database.connection.get_connection()
    Input: service_interest, contact_type, neighborhood, crew_zone
    Output: Top 2 job records with similarity scores (RealDictCursor rows)

   --- PARALLEL END ---

7. Agent 2 Step 2: Anthropic API call (Sonnet, no web search)
   Input: Raw job records from 6b
   Output: Formatted matches JSON (descriptions, match_confidence,
           estimated_annual_value)

8. Template selection: select_template(contact_type,
   research_confidence, match_confidence) -> (template_set, variant)

9. Agent 3: Anthropic API call (Opus, no web search)
   Input: Contact record + lead source opener + Agent 1 output +
          Agent 2 output + template instructions
   Output: Email JSON (subject, body)

10. Gmail API: Create draft via auth.get_client("google")
    Output: draft_id, gmail_link

11. Slack API: Post Block Kit message to #sales via auth.get_client("slack")
    Input: Contact summary + confidence scores + match summary +
           estimated deal value + gmail_link + hubspot_contact_url

12. Write outreach_drafts record to PostgreSQL
13. Register cross_tool_mapping entry (DRAFT -> gmail draft ID)
14. Update automation runner watermark for HubSpot contacts
```

---

## 11. Guardrails Summary

### Research Agent (Agent 1)

| Category | Rule |
|----------|------|
| Allowed | Business type, employee count, years in operation, review sentiment, neighborhood characteristics, property types, community context |
| Blocked | Personal social media, family details, property values, financial info, political/religious affiliations, health info |
| Fallback | If search finds nothing: return `research_confidence: "low"` and minimal output |

### Similar Jobs (Agent 2)

| Category | Rule |
|----------|------|
| Privacy | Never expose matched client's name or address in formatted output |
| Fallback | If no jobs match: return empty array with `match_confidence: "low"` |
| Scoring | Skip geographic scoring tier entirely if no address provided |

### Email Synthesis (Agent 3)

| Category | Rule |
|----------|------|
| Content | Never mention AI, research, or automation |
| Content | Never use matched client's name |
| Content | Never repeat lead's first name more than once in body |
| Content | Max 150 words (A/B variants), max 100 words (C variant) |
| Tone | Must read as if Maria wrote it in under 2 minutes |
| Fallback | If generation fails, skip draft, post Slack alert with raw contact info |

### System-Level

| Category | Rule |
|----------|------|
| Never auto-send | Always create draft, never send. Maria reviews every email. |
| Duplicate prevention | If a contact already has a Gmail draft (tracked in cross_tool_mapping), skip. |
| Rate limiting | If 10+ new contacts arrive in a single poll cycle, process sequentially with 5-second delay between chains to avoid API throttling |
| Cost ceiling | Log per-lead cost (input/output tokens). Alert if single lead exceeds $0.50 (Opus pricing). |

---

## 12. Database Changes Required

**Database:** PostgreSQL via psycopg2 (RealDictCursor). All new code uses `database.connection.get_connection()`.
**Migration:** Add `outreach_drafts` table via `automations/migrate.py` (consistent with existing automation table migrations).

### New Table: `outreach_drafts`

```sql
CREATE TABLE IF NOT EXISTS outreach_drafts (
    id TEXT PRIMARY KEY,                    -- SS-DRAFT-NNNN format
    hubspot_contact_id TEXT NOT NULL,
    contact_name TEXT NOT NULL,
    contact_email TEXT NOT NULL,
    contact_type TEXT,                      -- residential, commercial, hybrid
    template_set TEXT NOT NULL,             -- residential, commercial, hybrid
    template_variant TEXT NOT NULL,          -- A, B, C
    lead_source TEXT,
    research_confidence TEXT,               -- high, medium, low
    match_confidence TEXT,                  -- high, medium, low
    estimated_annual_value REAL,
    gmail_draft_id TEXT,
    gmail_link TEXT,
    slack_message_ts TEXT,                  -- Slack message timestamp for threading
    agent_1_output JSONB,                   -- Full JSON (for debugging/iteration)
    agent_2_output JSONB,                   -- Full JSON
    agent_3_output JSONB,                   -- Full JSON
    total_tokens_used INTEGER,
    estimated_cost_usd REAL,
    status TEXT DEFAULT 'completed',        -- completed, failed, skipped
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**PostgreSQL-specific notes:**
- Agent output columns use `JSONB` (not `TEXT`) for native JSON querying. This lets you run analytics directly: e.g., `SELECT agent_3_output->>'variant_used', COUNT(*) FROM outreach_drafts GROUP BY 1`.
- `NOW()` replaces SQLite's `datetime('now')`.
- `CREATE TABLE IF NOT EXISTS` matches the pattern in `automations/migrate.py`.

### Canonical ID Generation

Follow the collision-safe pattern from the existing codebase. Query both the `outreach_drafts` table and `cross_tool_mapping` for the highest existing DRAFT sequence number, then increment:

```python
def _next_draft_canonical_id(db):
    """Return the next available SS-DRAFT-NNNN canonical ID."""
    cur = db.cursor()

    cur.execute("SELECT id FROM outreach_drafts ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    table_max = int(row["id"].split("-")[-1]) if row else 0

    cur.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'DRAFT' ORDER BY canonical_id DESC LIMIT 1"
    )
    row2 = cur.fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    next_n = max(table_max, mapping_max) + 1
    return f"SS-DRAFT-{next_n:04d}"
```

This table serves double duty: operational tracking (did the draft get created?) and analytics (which template sets and variants convert best once Maria starts sending them?).

### Cross-Tool Mapping Entry

Each successful draft also creates a `cross_tool_mapping` entry linking the HubSpot contact ID to the Gmail draft ID, enabling the duplicate prevention check.

---

## 13. File Structure

```
automations/
    automation_07_sales_outreach.py     -- Main orchestrator
    agents/
        research_agent.py               -- Agent 1 (web search)
        similar_jobs_agent.py           -- Agent 2 (SQL + formatting)
        email_synthesis_agent.py        -- Agent 3 (draft generation)
    templates/
        lead_source_openers.py          -- LEAD_SOURCE_OPENERS dict
        template_selector.py            -- select_template() logic
        signatures.py                   -- Signature blocks by template set
```

---

## 14. Testing Checklist

| # | Test | Expected Outcome |
|---|------|-----------------|
| 1 | Commercial lead with full data (name, company, address, type, source) | Template set = commercial, variant based on confidence, full draft with industry-specific language |
| 2 | Residential lead with full data | Template set = residential, warm tone, neighborhood references |
| 3 | Lead with missing contact_type | Template set = hybrid, neutral language, no assumptions |
| 4 | Lead with no address | Agent 2 skips geographic scoring, Agent 1 searches by name/company only |
| 5 | Lead with no company name | Agent 1 switches to residential/neighborhood search strategy |
| 6 | Referral lead with referrer name available | Opening line includes referrer name |
| 7 | Referral lead without referrer name | Generic referral opener used |
| 8 | Agent 1 API timeout | Graceful fallback, research_confidence = low, draft still generated |
| 9 | Agent 2 returns no matching jobs | match_confidence = low, variant C selected, no job references in draft |
| 10 | Agent 3 returns invalid JSON | Draft skipped, Slack alert posted with raw contact info |
| 11 | Duplicate contact (draft already exists) | Automation skips, no duplicate draft |
| 12 | Batch arrival (10+ contacts in one poll) | Sequential processing with 5-second delays, all drafts created |
| 13 | Gmail deep link opens correctly | Draft opens in compose view in Google Workspace |
| 14 | Slack message renders correctly | Block Kit layout displays all fields, buttons link correctly |
| 15 | Both confidence scores high | Variant randomly alternates between A and B across multiple runs |

---

## 15. Prerequisites (Before Build Begins)

These must be resolved before implementation starts. Each is a one-time task.

| # | Task | Why | Blocking |
|---|------|-----|----------|
| 1 | Add `anthropic` to `requirements.txt` | SDK needed for web search tool integration (Agent 1). Currently not listed. The intelligence layer uses raw `requests` calls, but the Anthropic SDK's native `tools` parameter is cleaner for web search. | Agents 1, 2, 3 |
| 2 | Add `gmail.compose` scope to `auth/google_auth.py` | Without this scope, `users.drafts.create` will return 403. Adding a new scope invalidates the existing `token.json` and forces a one-time re-auth. | Gmail draft creation |
| 3 | Run `automations/migrate.py` update for `outreach_drafts` table | New PostgreSQL table. Follow existing pattern in `migrate.py`. | Entire automation |
| 4 | Verify `google-api-python-client` can build a Gmail service object via the existing `auth.get_client("google")` flow | The current auth may only build Drive/Docs/Sheets/Calendar services. Confirm it can also return a Gmail-scoped service, or add a `service_name` parameter. | Gmail draft creation |
| 5 | Test Gmail deep link format with Workspace account | Run a manual `users.drafts.create` call, build the URL `https://mail.google.com/mail/u/0/#drafts?compose={draft_id}`, and confirm it opens the draft in compose view. | Slack notification UX |

### Codebase Patterns to Follow

All new code in this automation must follow the production patterns, not the legacy one-off scripts at the repo root (`create_hubspot_contact.py`, `create_contact_priya_nair.py`, etc.). Specifically:

- **Auth:** Use `auth.get_client(tool_name)` for all tool interactions. Do not import `credentials.py` directly.
- **Database:** Use `database.connection.get_connection()` which returns a psycopg2 connection with `RealDictCursor`. All rows are dict-like. All placeholders are `%s`.
- **Base class:** Extend `automations/base.py` for the main orchestrator. The base class provides the `db` connection, logging, and dry-run support.
- **Migration:** Add new tables via `automations/migrate.py`, not standalone SQL scripts.
- **Canonical IDs:** Use `db/mappings.py` for `generate_id()`, `link()`, `lookup()`. The `_next_draft_canonical_id()` helper in Section 12 follows the same collision-safe pattern used in the codebase.

---

## 16. Future Enhancements (Out of Scope for Initial Build)

- **A/B tracking:** Once Maria sends drafts, track open/reply rates by template set and variant. Feed results back into selection logic.
- **Auto-detect contact type from research:** If Agent 1 infers the lead is commercial (found a business) but HubSpot says residential (or is blank), flag the mismatch in Slack.
- **Multi-language support:** If research detects a Spanish-language business, offer a Spanish draft option.
- **Follow-up automation:** If a draft sits unsent for 24+ hours, nudge Maria via Slack. If sent but no reply in 72 hours, auto-queue a follow-up draft.
