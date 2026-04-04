# Similar Jobs Matching — Enhanced Design Spec
**Date:** 2026-04-03
**File:** `automations/agents/similar_jobs_agent.py`

---

## Context

The Slack notification for Automation #7 (Sales Outreach) shows a "Similar jobs we have done" section. It currently returns **"No strong match"** for nearly every lead because:

1. The SQL query filters `WHERE j.status = 'completed'` — in a fresh or early-simulation environment, no completed jobs exist, so the query returns zero rows.
2. Scoring is done entirely in SQL with coarse signals (service type + neighborhood + zone + recency). No property subtype matching (medical, retail, office, etc.).
3. The Sonnet descriptions receive no property type context, so they default to generic phrases like "commercial-nightly in East Austin."

**Goal:** Ensure the matching reliably surfaces relevant past/current jobs for every lead, with richer descriptions that mention the property type.

---

## Scope

**One file modified:** `automations/agents/similar_jobs_agent.py`

No schema changes. No new dependencies. No changes to `slack_sales_notify.py` — the richer Sonnet description flows through the existing `>match_desc` block automatically.

---

## Design

### 1. SQL Query — Expanded Status Filter, Raw Fetch

Replace the current single SQL query (which scored inside SQL) with a simpler fetch query. Scoring moves to Python.

**Changes:**
- `WHERE j.status = 'completed'` → `WHERE j.status IN ('scheduled', 'completed')`
- Remove all `CASE` scoring logic from SQL
- Add `j.status`, `j.address AS job_address`, `c.company_name`, `c.address AS client_address` to SELECT
- `LIMIT 2` → `LIMIT 10` (Python re-ranks; SQL just fetches candidates)
- `ORDER BY similarity_score DESC` → `ORDER BY j.scheduled_date DESC`

```sql
SELECT
    j.id                AS job_id,
    j.service_type_id,
    j.scheduled_date,
    j.status,
    j.address           AS job_address,
    c.neighborhood,
    c.client_type,
    c.company_name,
    c.address           AS client_address,
    cr.zone             AS crew_zone,
    inv.amount          AS job_total
FROM jobs j
JOIN clients c  ON c.id = j.client_id
LEFT JOIN crews cr ON cr.id = j.crew_id
LEFT JOIN (
    SELECT job_id, MAX(amount) AS amount
    FROM invoices GROUP BY job_id
) inv ON inv.job_id = j.id
WHERE j.status IN ('scheduled', 'completed')
ORDER BY j.scheduled_date DESC
LIMIT 10
```

### 2. Property Type Inference

New function `_infer_property_type(client_type: str, company_name: str | None) -> str`.

Runs on both the DB candidate rows and the incoming lead contact dict. Returns one of:

| Return value | Conditions |
|---|---|
| `"home"` | `client_type == "residential"` (or `"one-time"`) |
| `"medical"` | commercial + any of: dental, dentist, orthodontic, medical, clinic, health, therapy, wellness, hospital |
| `"restaurant"` | commercial + any of: restaurant, cafe, bar, kitchen, grill, eatery, diner |
| `"retail"` | commercial + any of: boutique, salon, spa, shop, store, market |
| `"office"` | commercial + any of: office, consulting, law, accounting, financial, realty, insurance |
| `"commercial"` | commercial, no keyword match (fallback) |

Matching is case-insensitive substring search on `company_name`. For the incoming lead, `company_name` comes from `contact.get("company") or contact.get("company_name") or ""`.

### 3. Python Scoring — `_score_candidate(lead_ctx, row)`

After the SQL fetch, score each of the 10 rows in Python and take the top 2.

**`lead_ctx` dict (built once before the loop):**
```python
{
    "service_interest": contact.get("service_interest") or "",
    "contact_type":     contact.get("contact_type") or "",
    "property_type":    _infer_property_type(...),  # lead's inferred type
    "neighborhood":     contact.get("neighborhood") or "",
    "crew_zone":        _derive_crew_zone(neighborhood, address),  # existing helper
    "zip_prefix":       _extract_zip_prefix(contact.get("zip") or contact.get("address") or ""),
}
```

**Scoring table (max 100 pts):**

| Dimension | Condition | Points |
|---|---|---|
| **Service match** (max 40) | `service_type_id == service_interest` AND `client_type == contact_type` | 40 |
| | `client_type == contact_type` only | 20 |
| | Neither | 0 |
| **Property type** (max 20) | Exact property type match | 20 |
| | Both commercial, different subtype | 10 |
| | Mismatch | 0 |
| **Geography** (max 25) | `c.neighborhood == lead.neighborhood` (exact, case-insensitive) | 25 |
| | `cr.zone == lead.crew_zone` | 15 |
| | ZIP prefix match (first 3 digits) | 12 |
| | Else | 0 |
| **Recency** (max 15) | `scheduled_date` ≤ 30 days ago | 15 |
| | ≤ 90 days ago | 10 |
| | ≤ 180 days ago | 5 |
| | Older | 0 |

**Confidence thresholds (unchanged):**
- ≥ 80 pts → `"high"`
- ≥ 50 pts → `"medium"`
- < 50 pts → `"low"`

**New helper:** `_extract_zip_prefix(text: str) -> str` — extracts the first 5-digit sequence from an address string and returns the first 3 characters as a ZIP prefix (e.g., `"781"` from `"2401 Westlake Dr, Austin TX 78746"`). Returns `""` if no 5-digit sequence found.

### 4. Sonnet Prompt Enhancement

The JSON payload sent to Sonnet gains two new fields per job:
- `"property_type"`: the inferred subtype (e.g., `"medical"`, `"home"`)
- `"job_status"`: `"scheduled"` or `"completed"`

The system prompt gains one additional rule:
> "Mention the property type naturally in the description (e.g., 'a dental office', 'a family home', 'a retail boutique'). If job_status is 'scheduled', say the job is ongoing or currently active rather than past tense."

**No changes to `slack_sales_notify.py`** — richer descriptions flow through the existing `>match_desc` block.

---

## Data Flow (after change)

```
find_similar_jobs(contact)
  │
  ├─ build lead_ctx (service_interest, contact_type, property_type, neighborhood, crew_zone, zip_prefix)
  │
  ├─ SQL: fetch top 10 non-cancelled jobs (scheduled + completed), ordered by recency
  │
  ├─ Python: score each row → _score_candidate(lead_ctx, row)  [max 100 pts]
  │             ├─ service match (40 pts)
  │             ├─ property type (20 pts)
  │             ├─ geography (25 pts)
  │             └─ recency (15 pts)
  │
  ├─ Sort by score DESC → take top 2
  │
  ├─ Sonnet: format top 2 with property_type + job_status context
  │
  └─ Return { matches, match_confidence, estimated_annual_value }
```

---

## Files Changed

| File | Change |
|---|---|
| `automations/agents/similar_jobs_agent.py` | Replace `_SIMILARITY_SQL` + add `_infer_property_type`, `_extract_zip_prefix`, `_score_candidate`, `_build_lead_ctx`; update `_SYSTEM_PROMPT`; rewrite `find_similar_jobs` body |

---

## Verification

1. Run `python -m pytest tests/test_automations/ -v` — ensure existing tests pass
2. Dry-run automation 07: `python -m automations.runner --poll --dry-run`
3. Check the Slack #sales notification — "Similar jobs we have done" should now show a job description that mentions property type (e.g., "a dental office" or "a family home")
4. Test with a lead whose `contact_type = "residential"` and one with `contact_type = "commercial"` — verify property type appears correctly in both descriptions
5. Confirm "No strong match" no longer appears for leads when the DB has any non-cancelled jobs
