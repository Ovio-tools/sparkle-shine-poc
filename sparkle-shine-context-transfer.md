# Sparkle & Shine POC - Context Transfer Document

**Last updated:** March 17, 2026
**Project owner:** OV (OVIO Digital)
**Purpose:** Transfer full project context to a new chat session

---

## 1. What This Project Is

An internal proof-of-concept (POC) that demonstrates the power of workflow automation and AI-driven business intelligence for small/medium service businesses. The POC simulates a fictional cleaning company across 8 real SaaS platforms + Google Workspace, pre-seeded with 12 months of realistic data. Cross-tool automations connect the tools. An intelligence layer powered by Claude (Anthropic API) delivers a daily morning briefing via Slack.

**Primary goal:** Validate the core product concept of OVIO Digital's AI consultancy before selling it to real clients.

**What this is NOT:** A customer-facing product. It's an internal asset to prove the concept works.

---

## 2. The Fictional Company

**Name:** Sparkle & Shine Cleaning Co.
**Location:** Austin, TX
**Revenue:** ~$2M/year
**Owner:** Maria Gonzalez (fictional)
**Services:** Residential cleaning (recurring + one-time) and commercial cleaning (contract-based)

### Team (18 employees)
- 12 cleaners in 4 crews (3 per crew)
- 2 team leads (each overseeing 2 crews)
- 1 office manager
- 1 sales/estimator
- 1 bookkeeper
- 1 owner (Maria)

### Crew Zones
- Crew A: Westlake/Tarrytown area
- Crew B: East Austin/Mueller
- Crew C: South Austin/Zilker
- Crew D: Round Rock/Cedar Park (evening commercial routes)

### Service Types
- Standard Residential Clean ($150, 120 min)
- Deep Clean ($275, 210 min)
- Move-In/Move-Out Clean ($325, 240 min)
- Recurring Residential: Weekly ($135), Biweekly ($150), Monthly ($165)
- Commercial Nightly Clean (per sq ft)
- Commercial Weekend Deep Clean (per sq ft)
- Commercial One-Time Project (custom)

---

## 3. The 12-Month Business Narrative

All seeded data tells this story:

| Months | Period | What Happens |
|--------|--------|-------------|
| 1-2 | Apr-May 2025 | Steady ops. 140 residential clients, 7 commercial contracts, 14 employees, ~$140K/month revenue |
| 3-4 | Jun-Jul 2025 | Summer surge. Deep clean spike. 2 new hires. "Spring into summer" email campaign: 25 leads, 18 convert. 2 commercial contracts don't renew (relocations) |
| 5-6 | Aug-Sep 2025 | Rough patch. 2 cleaners quit. Scheduling tight. 3 negative reviews + cancellations. Maria raises rates +$10/visit, causing 5 more cancellations |
| 7-8 | Oct-Nov 2025 | Stabilization. Replacement hires. Big new commercial win (medical office, $4,500/mo). Referral program launches: 15 new residential clients. Holiday pre-bookings start |
| 9-10 | Dec-Jan 2026 | Holiday peak then January dip. Revenue peaks in December. 2 commercial clients pay late (50-60 days on net-30 terms). Cash flow tightens |
| 11-12 | Feb-Mar 2026 | Recovery. Spring pipeline building. 12 active commercial proposals. "New year fresh start" Mailchimp campaign |

---

## 4. Tool Stack (Finalized)

| Function | Tool | Auth Method | Token Expiry | Cost |
|----------|------|-------------|-------------|------|
| Operations & Scheduling | Jobber | OAuth 2.0 | 60 min (refresh long-lived) | Free (90-day dev account) |
| Sales Pipeline | Pipedrive | API token (x-api-token header) | Never | Free (sandbox) |
| Marketing & Contacts | HubSpot Free CRM | Private App Token (Bearer) | Never | Free |
| Email Marketing | Mailchimp | API key (Basic Auth) | Never | Free or $13/mo |
| Finance | QuickBooks Online | OAuth 2.0 | 60 min (refresh 100 days) | Free (sandbox, 2 years) |
| Back-Office Tasks | Asana | Personal Access Token (Bearer) | Never | Free |
| Internal Comms & Briefings | Slack | Bot User OAuth Token (Bearer) | Never | Free |
| Business Knowledge | Google Workspace (Drive, Docs, Sheets, Calendar, Gmail) | OAuth 2.0 (Desktop app) | 60 min (refresh long-lived*) | Free |

*Google tokens expire after 7 days if the OAuth app is still in "Testing" mode. Publishing the app (even without verification) fixes this.

**Total monthly cost: $0 to $13**

### Tool Responsibility Split: Pipedrive vs. HubSpot
- **Pipedrive** owns the active sales process: deal pipeline, sales activities, commercial proposals, win/loss
- **HubSpot** owns marketing and the full contact database: lifecycle stages, lead sources, campaign integration with Mailchimp
- The overlap is intentional -- mirrors real SMB operations and creates a compelling demo for the intelligence layer detecting gaps between systems

### Why No Dedicated HR Tool
- Homebase (the ICP's likely HR tool) locks API access behind its $100/month All-in-One plan
- Decision: Model employee/crew data within Jobber (native crew assignments, job durations) + Google Sheets for payroll-adjacent data
- This keeps the tool count focused on tools with free/dev-tier API access

---

## 5. Master Data Model

### 15 Core Entities

| Entity | Description | Source Tools |
|--------|-------------|-------------|
| Client | Residential + commercial sub-types | Jobber, HubSpot, Pipedrive, QuickBooks, Mailchimp |
| Lead / Prospect | Pre-conversion contacts | HubSpot, Pipedrive |
| Job | Single service visit | Jobber |
| Recurring Service Agreement | Templates for recurring jobs | Jobber |
| Commercial Proposal | Quote/bid for commercial contracts | Pipedrive, Jobber |
| Employee | Staff profiles and roles | Jobber, Google Sheets |
| Crew | Grouping of employees | Jobber, Google Sheets |
| Invoice | Billing records | QuickBooks, Jobber |
| Payment | Payment receipts | QuickBooks |
| Marketing Campaign | Email campaigns | Mailchimp |
| Marketing Interaction | Opens, clicks, conversions | Mailchimp, HubSpot |
| Review / Rating | Post-job feedback | Google Sheets (simulated) |
| Task | Back-office to-dos | Asana |
| Calendar Event | Meetings, site visits | Google Calendar |
| Business Document | SOPs, contracts, templates | Google Drive / Docs |

### Cross-Tool Identity Mapping
- Central SQLite database holds canonical records with cross-system IDs
- Email address is the primary natural key for residential clients
- Commercial clients use compound key (company name + billing contact email)
- Format: SS-{TYPE}-{SEQUENTIAL}, e.g., SS-CLIENT-0047, SS-JOB-0001

### Data Volume Targets

| Data Type | Volume | Notes |
|-----------|--------|-------|
| Residential clients (total) | 300+ | ~180 active, ~60 churned, ~40 occasional, ~20 quick churn |
| Commercial clients (active) | 8-10 | Plus 2 churned mid-year |
| Unconverted leads | 150+ | In HubSpot/Pipedrive only |
| Commercial proposals | 40-50 | Full bid-to-contract lifecycle |
| Employees | 18 | 4 crew groupings |
| Jobs (12 months) | ~8,200 | Residential recurring + one-time + commercial |
| Invoices | ~8,200 | Mirrors job count |
| Mailchimp contacts | 250-450 | Depends on plan |
| Email campaigns | 4-6 | Seasonal and promotional |
| Asana tasks | 200-300 | Across 4 projects |
| Google Docs | 10-15 | SOPs, contracts, templates |
| Google Sheets | 4-6 | Rate card, budget, inventory |

---

## 6. Commercial Bid-to-Contract Workflow

Full lifecycle modeled: Inquiry (HubSpot) -> Site Visit (Jobber + HubSpot) -> Proposal/Quote (Jobber + Pipedrive) -> Negotiation (Pipedrive) -> Contract Signed (triggers in HubSpot, Jobber, QuickBooks, Mailchimp, Slack simultaneously) -> Ongoing Service

For data seeding: ~40-50 inquiries, 30 site visits, 25 proposals sent, 10 contracts won, 15 lost (varied reasons), 2 churned mid-year.

---

## 7. Asana Onboarding Automation

When a deal closes in Pipedrive, auto-create task lists in Asana based on client type:

**Residential (9 tasks):** Welcome email, collect payment method, create Jobber profile, schedule first visit, confirm details, post-clean follow-up call, review request email, rebooking check (7 days), confirm recurring schedule (14 days)

**Commercial (10 tasks):** Send contract copy, set up QuickBooks billing (net-30), create Jobber recurring schedule, pre-service walkthrough with crew lead, order specialized supplies, first service visit, quality inspection, client check-in (2 days), 30-day review, 90-day formal review

**One-Time (5 tasks):** Confirm appointment, complete service, send invoice, post-service follow-up, "convert to recurring" offer (5 days)

Asana has 4 projects: Sales Pipeline Tasks, Marketing Calendar, Admin & Operations, Client Success.

---

## 8. Data Seeding Approach

**Strategy:** API-first (push directly to each tool's API)
**LLM usage:** Claude generates realistic text content (names, job notes, reviews, Asana task descriptions, Pipedrive activity notes, Google Docs content)
**Structured fields:** Generated programmatically using distributions and the business narrative

### 9-Pass Generation Order
1. Timeline backbone (narrative as data structure)
2. Employees and crews (with hire/quit dates matching narrative)
3. Clients (300+ residential with Austin neighborhoods, 40-50 commercial inquiries)
4. Jobs (~8,200, derived from clients and crews with variance rolls)
5. Financial data (invoices, payments with realistic lag distributions, expenses)
6. Marketing and sales data (Mailchimp campaigns, HubSpot lifecycle, Pipedrive deals)
7. Asana tasks (200-300 across 4 projects, deliberate overdue patterns for Maria)
8. Google Workspace content (SOPs, contracts, rate cards, calendar events)
9. Anomalies and Easter eggs (planted patterns for intelligence layer to discover)

### Planted Discovery Patterns
- One crew takes 20% longer but has highest satisfaction (quality vs. speed tradeoff)
- Referral clients have 2x retention rate vs. Google Ads clients
- Tuesday/Wednesday jobs have 30% lower complaint rates
- Commercial client requesting monthly add-ons (upsell signal)
- 3 Westlake neighborhood cancellations in 2 weeks (possible competitor)
- Maria's Asana overdue rate is 40% vs. office manager's 10% (delegation insight)
- Referral program leads have 30% higher contract value

---

## 9. Intelligence Layer Architecture

### Scope
- **In scope:** Daily morning briefing via Slack (MVP)
- **Out of scope:** Chatbot / conversational interface (explicitly removed from scope)
- **LLM:** Claude via Anthropic API (claude-sonnet-4-20250514 for cost efficiency, claude-opus-4-6 available for complex analysis)

### Pipeline
Nightly batch: Sync all tools (2:00 AM) -> Compute metrics (SQL/Python) -> Generate briefing (Claude) -> Deliver to Slack (by 6:00 AM)

### Data Ingestion
- Nightly sync from all 8 platforms into local SQLite database
- Each tool has a dedicated syncer module
- Google Workspace: simple text extraction + keyword indexing (not embeddings)
- Gmail: metadata only (sender, recipient, subject, date)

### Analysis Engine
**Computed metrics (deterministic):** Revenue (daily/weekly/monthly, vs. target, by type), operations (completion rate, duration variance, utilization), sales (conversion rate by source, pipeline value), financial health (cash, AR aging, DSO, margins), marketing (campaign ROI, engagement), tasks (overdue counts, completion rates)

**Synthesized observations (Claude):** Cross-tool pattern detection, anomaly flagging with context, opportunity identification, delegation insights, predictive flags

### Daily Briefing Format
1. Yesterday's performance (jobs, revenue, notable events)
2. Cash position (bank balance, outstanding invoices, overdue targets)
3. Today's schedule (jobs, crews, gaps, staffing issues)
4. Sales pipeline (new leads, deal movement, stale deals)
5. Action items (ranked by urgency and impact)
6. One opportunity (high-value insight to close on a forward-looking note)

### Google Workspace Document Integration (POC scope)
- Simple text extraction and keyword matching (NOT full RAG)
- Key documents pulled from Drive, text stored in SQLite document_index table
- Intelligence layer searches for relevant excerpts using keyword queries
- Full RAG with embeddings reserved for Phase 6

### Future Phase 6: Gemini Embedding 2
- Google's natively multimodal embedding model can embed images, PDFs, and text into shared vector space
- Would enable retrieval from photos, scanned documents, annotated floor plans
- Natural fit with Google Workspace ecosystem
- Parked for later; documented as future enhancement

---

## 10. Cross-Tool Automations (6 Priority)

1. **New Client Onboarding** (Trigger: deal closed in Pipedrive) -> Asana task list + Jobber client/job + QuickBooks customer/invoice template + Mailchimp tags + Slack notification
2. **Job Completion Flow** (Trigger: job complete in Jobber) -> QuickBooks invoice + Mailchimp review request (2-day delay) + HubSpot engagement update + Slack summary
3. **Payment Received** (Trigger: payment in QuickBooks) -> Pipedrive deal update + HubSpot financial data + Slack notification
4. **Lead Leak Detection** (Scheduled: daily) -> Find HubSpot leads with no Pipedrive deal + Slack alerts + Asana tasks
5. **Overdue Invoice Escalation** (Scheduled: weekly) -> QuickBooks AR aging -> Asana tasks (30+ days) + Slack alerts (60+ days)
6. **Negative Review Response** (Trigger: rating <= 2 stars) -> Slack alert + Asana task + HubSpot flag

---

## 11. Phased Build Plan

| Phase | Name | Timeline |
|-------|------|----------|
| 0 | Day-Zero API Verification | 2-3 days (COMPLETE - all tools verified) |
| 1 | Foundation & Company Setup | Week 1-2 |
| 2 | Data Seeding | Week 2-4 |
| 3 | Cross-Tool Automations | Week 4-6 |
| 4 | Intelligence Layer (Daily Briefing) | Week 6-8 |
| 5 | Polish & Demo Prep | Week 8-9 |
| 6 (Future) | Enhanced Document Intelligence (RAG, Gemini Embedding 2) | TBD |

---

## 12. Phase 0 Status: COMPLETE

All 8 tools have been walked through with setup instructions. Here's where things stand:

| Tool | Account Created | API Verified | Key Details |
|------|----------------|-------------|-------------|
| Pipedrive | Sandbox account | Yes | API token in header, test person + deal created |
| Jobber | Need to create | Pending | Developer Center + dev testing account (90 days). OAuth 2.0 required. Email api-support@getjobber.com for extension. Dev signup: getjobber.com/developer-sign-up/ |
| QuickBooks | Need to create | Pending | Intuit Developer portal. Sandbox lasts 2 years. Sandbox base URL: sandbox-quickbooks.api.intuit.com. Core API calls (create/update) free, CorePlus (reads) metered |
| Asana | Need to create | Pending | Simplest setup. PAT never expires. 150 req/min on free plan. Create 4 projects: Sales Pipeline Tasks, Marketing Calendar, Admin & Operations, Client Success |
| HubSpot | Need to create | Pending | Private App token never expires. Free plan allows 2 users. Custom properties needed for client_type, service_frequency, lead_source, etc. |
| Mailchimp | Need to create | Pending | API key never expires. Free plan: 250 contacts, 500 sends/month. May need Essentials ($13/mo) for full contact universe. Server prefix from URL (e.g., us6) |
| Slack | Need to create | Pending | Bot token never expires. Channels: #daily-briefing, #new-clients, #operations, #sales. Bot needs chat:write, channels:read, channels:join, channels:history, im:write scopes |
| Google Workspace | Need to create | Pending | OAuth Desktop app. Enable: Drive, Docs, Sheets, Calendar, Gmail APIs. Publish app to avoid 7-day token expiry in Testing mode |

---

## 13. Phase 1 Directive: What Needs to Be Built

Phase 1 has 8 steps (a detailed directive document was being generated when this context doc was requested):

**Step 1:** Initialize Python project structure (directories, .env, requirements.txt, credentials loader)

**Step 2:** Define business configuration (config/business.py with company profile, 18 employees, 4 crews, 9 service types, 12 neighborhoods; config/narrative.py with 12-month timeline)

**Step 3:** Build SQLite database schema (15 entity tables + cross_tool_mapping + daily_metrics_snapshot + document_index tables)

**Step 4:** Build cross-tool ID mapping system (SS-TYPE-NNNN format, link/lookup/reverse lookup methods, find_unmapped for sync gaps)

**Step 5:** Build authentication layer (simple_clients.py for 5 permanent-token tools, jobber_auth.py and quickbooks_auth.py for OAuth, google_auth.py wrapping existing flow, unified get_client(tool_name) interface)

**Step 6:** Configure each tool with company identity (Pipedrive pipeline stages + custom fields, HubSpot custom properties, Asana project sections + team members, Mailchimp merge fields + tags, QuickBooks service Items + expense categories, Slack channel topics)

**Step 7:** Populate Google Workspace (8 Google Docs with LLM-generated content: employee handbook, 3 SOPs, 2 contract templates, client-specific instructions, training checklist; 4 Google Sheets: rate card, budget tracker, supply inventory, vehicle maintenance; Calendar events for past 3 months)

**Step 8:** End-to-end integration test (15-check verification: credentials, database, mapping, auth for all 8 tools, config imports, tool IDs, Google content indexing)

---

## 14. Technical Architecture Decisions

- **Language:** Python (built with Claude Code)
- **Database:** SQLite (sparkle_shine.db)
- **No middleware:** All integrations are direct API calls (no Zapier/Make)
- **OAuth token storage:** Local JSON files (.jobber_tokens.json, .quickbooks_tokens.json, token.json for Google)
- **Tool ID registry:** config/tool_ids.json stores all tool-specific IDs (pipeline stages, custom field IDs, project GIDs, etc.) created during setup
- **LLM for data generation:** Claude (Anthropic API) generates realistic text fields during seeding
- **LLM for intelligence:** Claude generates daily briefing from structured context document

---

## 15. Key Design Principles

1. **Data realism matters most.** Seasonal patterns, churn cohorts, geographic clustering, payment lag distributions. The demo is only as good as the data.
2. **The master data model is the linchpin.** If Client #47 in Jobber doesn't map to the same person in HubSpot and QuickBooks, everything breaks.
3. **The overlap between tools is the point.** Real SMBs have messy, overlapping tool stacks. The intelligence layer's value is connecting what no single tool can see.
4. **This IS version 0.5 of the actual product.** The automations and intelligence layer built here are roughly what would be delivered to a real client.

---

## 16. Documents Already Created

1. **sparkle-shine-project-plan.docx** -- Comprehensive 11-section project plan covering everything above
2. **Phase 1 directive document** -- Was being generated when this context doc was requested (partially complete)

---

## 17. ICP Research Inputs That Shaped Decisions

- Target ICP: ~$2M cleaning company in Austin (validated through prospect interviews)
- ICP uses: Jobber for operations, Pipedrive for sales, HubSpot for marketing/contacts, Asana for back-office tasks, Google Workspace for docs/communication
- Mailchimp for email marketing (common in the segment)
- QuickBooks for accounting (standard for this business size)
- Slack for internal team communication
