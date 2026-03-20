# Sparkle & Shine POC — Demo Walkthrough
## For OVIO Digital prospect presentations
### Target duration: 20 minutes

---

## Pre-Demo Checklist (5 minutes before)

Run these commands before the prospect joins:

1. **Token preflight:**
   ```
   python -m demo.hardening.token_preflight
   ```
   (All 8 tools should show OK or WARN. If any FAIL, fix before starting.)

2. **Generate fresh briefing for today's demo date:**
   ```
   python -m intelligence.runner --skip-sync --date 2026-03-17
   ```
   (Confirm it posts to #daily-briefing in Slack.)

3. **Pre-generate the comparison scenarios (if not already cached):**
   ```
   python -m demo.scenarios.scenario_runner --scenario rough_patch
   python -m demo.scenarios.scenario_runner --scenario holiday_crunch
   ```

4. **Open these tabs in advance:**
   - Slack: #daily-briefing channel
   - Jobber: dashboard (logged in)
   - Pipedrive: deals pipeline view
   - HubSpot: contacts list
   - QuickBooks: invoice list
   - Asana: Sales Pipeline Tasks project
   - Terminal: ready to run commands

5. **Quick health check:**
   ```
   python -m demo.hardening.health_check
   ```
   (All 8 green means you're good to go.)

---

## ACT 1: The Problem (5 minutes)

### Beat 1.1 — Set the Scene

**TALKING POINT:**
"Let me introduce you to Maria. She runs Sparkle & Shine Cleaning, a $2M-a-year cleaning company here in Austin. 18 employees, 4 crews, 300 clients. She's doing well — but she's drowning in tools."

**SCREEN:**
Show nothing yet. This is a verbal setup.

**FALLBACK:**
No tech needed here. If you're nervous, use this beat to breathe and get settled.

**TRANSITION:**
"Let me show you what her day looks like."

---

### Beat 1.2 — The Tool Tour (rapid-fire, 60 seconds)

**TALKING POINT:**
"Maria uses 8 different tools to run her business. Every one of them has real data. Let me show you."

**SCREEN:**
Quick-switch through each tab (5–8 seconds per tool):

1. **Jobber** — Show the job calendar. Point out crew assignments.
   *"This is where her crews get their schedules."*
2. **Pipedrive** — Show the pipeline. Point out deal values.
   *"This is where she tracks commercial sales. 12 open proposals."*
3. **HubSpot** — Show the contacts list. Scroll briefly.
   *"300+ contacts with full lifecycle history."*
4. **QuickBooks** — Show the invoice list. Point out overdue column.
   *"And this is where the money lives."*

(Skip Mailchimp, Asana, Slack, and Google for now — too many tools in 60 seconds. They come up later.)

**FALLBACK:**
If any tool is slow to load, skip it and say: *"That one's loading — I'll come back to it."* Move to the next tab immediately.

**TRANSITION:**
"So Maria has all this data spread across 8 tools. The question is: how does she figure out what actually matters today?"

---

### Beat 1.3 — The Pain

**TALKING POINT:**
"Right now, Maria's answer is: she doesn't. She logs into each tool one at a time, mentally pieces things together, and hopes she doesn't miss anything. Last September, she missed a pattern of cancellations in Westlake — 3 clients in 2 weeks — that turned out to be a competitor moving in. She found out a month later."

**SCREEN:**
Stay on QuickBooks or whichever tool is currently visible. The point is the contrast with what comes next.

**FALLBACK:**
If you lose your train of thought, pause on the overdue invoices column in QuickBooks. *"The data is all here. But no one's connecting it."* That's the beat.

**TRANSITION:**
"What if she didn't have to do any of that? What if the answer was waiting in Slack when she woke up?"

---

## ACT 2: The Solution (10 minutes)

### Beat 2.1 — Today's Briefing

**TALKING POINT:**
"Every morning at 6 AM, Maria gets this."

**SCREEN:**
Switch to Slack. Open #daily-briefing channel. Show the most recent briefing (March 17, 2026 — recovery scenario). Scroll slowly through the 6 sections. Pause at each for 5–10 seconds:

- **Yesterday's Performance:** *"Specific numbers. Not guesses."*
- **Cash Position:** *"She knows her bank balance, her outstanding invoices, and who's paying late. Before she even opens QuickBooks."*
- **Today's Schedule:** *"Crew assignments, utilization rates. She can see if anyone's overloaded or underbooked."*
- **Sales Pipeline:** *"12 open proposals, 2 stale deals that need follow-up. The system flags them automatically."*
- **Action Items:** *"Ranked. Not a to-do list — a priority list."*
- **Opportunity:** *"This is the one thing she should think about today."*

**FALLBACK:**
If the Slack briefing isn't there, run it live from the terminal:
```
python -m intelligence.runner --skip-sync --date 2026-03-17
```
Say: *"Let me generate one right now so you can see it happen."* This actually makes for a more compelling demo — they see the pipeline run in real time.

**TRANSITION:**
"That's one day. But the real power is seeing how this adapts over time. Let me show you what happened last September."

---

### Beat 2.2 — The Rough Patch Contrast

**TALKING POINT:**
"In September, Maria had a rough month. Two cleaners quit, reviews tanked, she raised prices and lost 5 more clients. Here's what the AI would have told her."

**SCREEN:**
Open `demo/scenarios/output/rough_patch_briefing.md` in a text editor, or display it in terminal:
```
cat demo/scenarios/output/rough_patch_briefing.md
```

Point out:
- Revenue BELOW target (vs. today's "on track")
- Cancellation cluster alert in Westlake
- Staffing gap warnings
- Negative review flags
- Action items focused on retention, not growth

**TALKING POINT:**
"Compare this to today's briefing. Completely different tone, completely different priorities. The system adapts because the data changes."

**FALLBACK:**
If the scenario file is missing, generate it live:
```
python -m demo.scenarios.scenario_runner --scenario rough_patch
```
Takes ~15 seconds. Talk through the generation while it runs: *"You can see it pulling from each connected tool in sequence — Jobber, QuickBooks, HubSpot, Pipedrive."*

**TRANSITION:**
"Now let me show you December. Revenue at its highest all year — but the briefing tells a more complicated story."

---

### Beat 2.3 — The Holiday Cash Crunch

**TALKING POINT:**
"Here's December. Revenue peaked at its highest all year. You'd think everything is great. But look at what the AI flagged."

**SCREEN:**
Open `demo/scenarios/output/holiday_crunch_briefing.md`.

Point out:
- Revenue above target (good)
- But AR aging shows 60+ day balances (bad)
- Late payers flagged by name
- Cash position warning despite strong revenue

*"This is the kind of thing that sneaks up on a business owner. Revenue looks great. Cash flow is actually tightening."*

**FALLBACK:**
If the file is missing:
```
python -m demo.scenarios.scenario_runner --scenario holiday_crunch
```
While it runs: *"The December scenario is a good stress test — high volume, lots of invoices, a few slow payers. Watch what it surfaces."*

**TRANSITION:**
"So the intelligence adapts to the data. But what's connecting all of it under the hood? Let me show you."

---

### Beat 2.4 — The Planted Patterns *(optional — use if time allows)*

**TALKING POINT:**
"The system also spots patterns that no single tool can see. For example, it discovered that Maria's referral clients have twice the retention rate of her Google Ads clients. And that one of her crews takes 20% longer per job but has the highest customer satisfaction scores. Those are strategic insights — not just dashboard numbers."

**SCREEN:**
Point to the Opportunity section of any briefing that mentions these. Or speak to them verbally without switching screens.

**FALLBACK:**
Skip entirely if you're running long. This beat adds color but isn't load-bearing.

**TRANSITION:**
"That's the intelligence layer. Now let me show you the automation layer underneath it."

---

## ACT 3: The Infrastructure (5 minutes)

### Beat 3.1 — Cross-Tool Automations

**TALKING POINT:**
"The briefing is the most visible piece. But underneath it, there are 6 automations connecting these tools in real time."

**SCREEN:**
Trigger the Lead Leak Detection automation (scheduled daily check, safe to run live):
```
python -m automations.lead_leak_detection --run-once
```

Then:
1. Switch to Slack #operations — show the alert it posted.
2. Switch to Asana — show the task it created.

*"When a lead enters HubSpot but doesn't have a matching deal in Pipedrive, the system catches it, alerts the team in Slack, and creates a follow-up task in Asana. No one had to check."*

**FALLBACK:**
If the automation has no leads to flag (data is clean), don't force it. Show the Asana task list as evidence of past automation runs: *"You can see the history here — every time this fired, it left a task."*

**TRANSITION:**
"Six automations running 24/7, with zero manual triggers. But all of this only works if the underlying data is clean."

---

### Beat 3.2 — Data Reliability

**TALKING POINT:**
"All of this only works if the data is accurate. Let me show you the integrity audit."

**SCREEN:**
Open `demo/audit/audit_report.txt` (pre-generated). Show the summary: pass rate, tool breakdown.

*"We audit every record across all 8 tools. Right now, data integrity is at {pass_rate}%. That means when Maria sees a number in her briefing, she can trust it."*

- If pass_rate is 95%+: lean into it as a closing point.
- If pass_rate is lower: either skip this beat, or address it honestly — *"We're still tuning the [tool] connector. That's normal at this stage of a POC."*

**FALLBACK:**
If the audit report is stale or missing, skip this beat. Don't generate a fresh one mid-demo — it takes too long and shifts focus.

**TRANSITION:**
"So that's the full picture. Let me put it together."

---

### Beat 3.3 — The Close

**TALKING POINT:**
"That's the system. 8 tools connected, cross-tool automations running 24/7, and an AI analyst that synthesizes everything into 600 words Maria reads with her morning coffee. Every briefing costs about a penny."

*(Pause. Let it land.)*

"This is what we build for our clients at OVIO Digital. The tools change, the business changes — but the pattern is the same: connect your data, automate the busywork, and surface what matters."

**SCREEN:**
Back to the Slack briefing. Let it sit on screen while you talk. Don't click anything.

**FALLBACK:**
If the briefing looks thin or the Slack channel is cluttered, switch to the recovery scenario markdown file instead. Cleaner, easier to read at a glance.

---

## Emergency Fallbacks

| Problem | Fix |
|---------|-----|
| Slack briefing not showing | Run: `python -m intelligence.runner --skip-sync --date 2026-03-17` |
| Any tool's UI won't load | Skip that tool in the tour. The briefing itself is the star. |
| OAuth token expired mid-demo | Run: `python -m demo.hardening.token_preflight` (fixes it in ~10 sec) |
| Scenario briefing file missing | Run: `python -m demo.scenarios.scenario_runner --scenario {id}` |
| Pipeline crashes during live run | Switch to pre-generated output. Say: *"Let me show you the output from this morning's run."* |
| Prospect asks about cost | *"Each daily briefing costs about $0.01–0.02 in AI processing. The tools themselves are all free-tier or standard plans the business already pays for."* |
| Prospect asks about setup time | *"For a business like Maria's, initial setup is 4–6 weeks. The first week is connecting the tools and mapping the data. Then we seed the history and tune the briefings."* |
| Prospect asks "does this work with [other tool]?" | *"Yes. The architecture is tool-agnostic. We've built connectors for 8 tools here. Adding a new one — Housecall Pro, ServiceTitan, whatever you use — typically takes 2–3 days."* |

---

## Post-Demo Checklist

After the demo:

1. Send the prospect a copy of one briefing (the recovery scenario — it's the most balanced and impressive).
2. Note which beats got the strongest reaction.
3. Note any questions you couldn't answer.
4. If the prospect asked about a specific tool they use, research its API availability before the follow-up call.
