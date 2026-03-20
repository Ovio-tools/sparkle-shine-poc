# Regeneration Status Report — Sparkle & Shine POC

**Date:** 2026-03-18
**Scope:** Source fixes in `gen_clients.py` and `gen_financials.py` → full DB regeneration → validation

---

## Changes Applied

### Issue 1 — Commercial Pricing (gen_clients.py + gen_financials.py)

- Updated all 10 `_COMMERCIAL_CLIENTS` entries with corrected `monthly_value` fields (5–10× higher) and a new `schedule` field (`nightly_plus_saturday`, `nightly_weekdays`, `3x_weekly`, `2x_weekly`, `daily`).
- Added `_compute_commercial_rates()`, `_COMMERCIAL_RATES_BY_COMPANY` registry, and `get_commercial_per_visit_rate()` helper with nightly/Saturday split logic.
- Wired `_client_id_to_company` cache in `_gen_commercial()` for same-process resolution.
- In `gen_financials.py`: added import, added `j.scheduled_date` to jobs query, removed the old LTV÷job_count `commercial_price` block, replaced with `get_commercial_per_visit_rate()` call per invoice. Late-payer IDs now resolved by company-name lookup (resilient to regeneration). Revenue validation updated to use accrual accounting (all invoices by issue date).

### Issue 2 — Referral LTV Pattern (gen_clients.py)

- Added `_REFERRAL_ACQ_MONTHS`, `_REFERRAL_MONTH_WEIGHTS` (40% Apr–Sep / 60% Oct–Feb), `_REFERRAL_FREQ_WEIGHTS` (biweekly 45% / weekly 25% / monthly 30%).
- Updated `_gen_active_recurring()` 162-client loop: `acq_source` determined first; referral clients use their own month and frequency distributions; first 15 formal-program (Oct 2025+) referral clients tagged `referral_program: True`.
- Capped referral churn at ≤3 in both `_gen_churned_residential()` and `_gen_quick_churn()` (extras re-assigned to `organic search`).
- Fixed `_resolve_referrals()` to handle embedded `referral_pending` in notes (e.g. `"referral_program: True | referral_pending"`).

---

## Validation Results

### ✅ Check 1 — Commercial Invoice Rate Sanity: PASS

All 10 commercial clients priced via `get_commercial_per_visit_rate()`. Nightly/Saturday split correct for `nightly_plus_saturday` schedule clients.

| Client | Invoices | Min | Max | Avg | Total |
|---|---|---|---|---|---|
| Barton Creek Medical Group | 131 | $461.54 | $1,038.46 | $941.57 | $123,346 |
| Mueller Tech Suites | 206 | $538.46 | $538.46 | $538.46 | $110,923 |
| Crestview Coworking | 240 | $340.91 | $340.91 | $340.91 | $81,818 |
| Domain Business Center | 148 | $273.50 | $615.38 | $462.92 | $68,512 |
| South Lamar Dental | 131 | $461.54 | $461.54 | $461.54 | $60,462 |
| Rosedale Family Practice | 72 | $692.31 | $692.31 | $692.31 | $49,846 |
| Hyde Park Realty Group | 71 | $500.00 | $500.00 | $500.00 | $35,500 |
| North Loop Bistro | 39 | $423.08 | $423.08 | $423.08 | $16,500 |
| East Cesar Chavez Gallery | 52 | $158.33 | $158.33 | $158.33 | $8,233 |

No client is using the old LTV÷job_count flat rate.

---

### ❌ Check 2 — Monthly Revenue Targets: 1/12 PASS (need 10/12)

Revenue recognized by service/issue date (accrual).

| Month | Accrual Revenue | Target Range | Result |
|---|---|---|---|
| 2025-04 | $20,664 | $135,000–$145,000 | ❌ FAIL (−85%) |
| 2025-05 | $38,499 | $135,000–$145,000 | ❌ FAIL (−73%) |
| 2025-06 | $73,152 | $148,000–$160,000 | ❌ FAIL (−52%) |
| 2025-07 | $81,997 | $148,000–$160,000 | ❌ FAIL (−47%) |
| 2025-08 | $79,985 | $128,000–$140,000 | ❌ FAIL (−40%) |
| 2025-09 | $98,421 | $128,000–$140,000 | ❌ FAIL (−27%) |
| 2025-10 | $112,018 | $140,000–$155,000 | ❌ FAIL (−24%) |
| 2025-11 | $115,957 | $140,000–$155,000 | ❌ FAIL (−21%) |
| 2025-12 | $124,891 | $165,000–$185,000 | ❌ FAIL (−29%) |
| 2026-01 | $123,959 | $120,000–$135,000 | ✅ PASS |
| 2026-02 | $111,215 | $135,000–$150,000 | ❌ FAIL (−21%) |
| 2026-03 | $63,527 | $135,000–$150,000 | ❌ FAIL (partial month) |

**Root cause — structural, not a code bug.**

The monthly targets assume a fully-mature book of business from April 2025. The actual business grows into those numbers:

- **Apr 2025:** Only 3 small commercial clients are active (Crestview $7.5k/month, North Loop Bistro $5.5k, East Cesar Chavez $4.75k = $17.75k commercial) plus ~60 newly-acquired residential clients (~$9k residential). Ceiling ≈ $27k.
- **Full capacity (Oct 2025+):** 7–8 commercial clients running + 240 residential active = ceiling ≈ $124k/month — which barely touches the bottom of the $120k–$135k band (Jan 2026 PASS).

The business model as designed cannot generate $135k+ in the ramp-up months. The revenue calibration was never a pricing-only problem for the early months.

**Required action to hit 10/12:**

Option A — Revise the early-month target bands to reflect the ramp-up period:

| Month | Suggested Revised Target |
|---|---|
| 2025-04 | $18,000–$30,000 |
| 2025-05 | $35,000–$55,000 |
| 2025-06 | $65,000–$85,000 |
| 2025-07 | $75,000–$95,000 |
| 2025-08 | $75,000–$95,000 |
| 2025-09 | $90,000–$110,000 |

Option B — Add 2–3 larger commercial clients with April–June win dates so commercial revenue is material from month 1.

---

### ⚠️ Check 3 — Referral Pattern: 2/3 sub-checks PASS

| Sub-check | Value | Result |
|---|---|---|
| Referral churned clients ≤3 | 3 | ✅ PASS |
| Earliest active referral ≤ 2025-07-01 | 2025-04-12 | ✅ PASS |
| Referral LTV ≥1.5× Google Ads LTV | 0.84× ($1,712 vs $2,033) | ❌ FAIL |

**Root cause — internally inconsistent spec requirements.**

The spec requires 60% of referral clients to be acquired in Oct–Feb (max 5 months service history) while Google Ads clients are weighted toward Apr–Jun (~9 months avg). With identical per-visit prices, tenure dominates LTV:

```
Referral avg tenure ≈ 5.5 months × $300/month = $1,650 (ceiling with current distribution)
Required for 1.5×:   $2,033 × 1.5 = $3,050  →  requires ~10 months avg tenure
```

No frequency weighting (biweekly/weekly/monthly price mix) can close a 4-month average tenure gap. This was also noted in `REVENUE_CALIBRATION.md`:

> *"Recommend re-expressing this pattern through retention rate and churn differential rather than raw LTV, or explicitly assign longer tenure to referral clients in gen_clients.py so the pattern survives to invoice-derived LTV."*

**Options to resolve:**

1. **Change the check metric** — use churn rate differential (referral churn rate vs. Google Ads churn rate) or average tenure length instead of raw LTV. This is the more honest expression of the referral advantage given the program launched in Nov 2025.
2. **Flip the acquisition weights** — put 60% of referrals in Apr–Sep and 40% in Oct–Feb. Referral avg tenure rises to ~7.5 months → LTV ≈ $2,250, ratio ≈ 1.1× (still below 1.5×, but closer).
3. **Extend the simulation window** — if `TODAY` is moved to 2027-03-17, late referral clients accumulate 16+ months and the ratio inverts. Not a realistic near-term fix.

---

## Summary

| Check | Result | Blockers |
|---|---|---|
| Commercial invoice rate sanity | ✅ PASS | — |
| Monthly revenue targets (10/12) | ❌ FAIL (1/12) | Structural: targets assume mature business from day 1 |
| Referral churned ≤3 | ✅ PASS | — |
| Earliest referral ≤ 2025-07-01 | ✅ PASS | — |
| Referral LTV ≥1.5× Google Ads | ❌ FAIL (0.84×) | Structural: spec's late-weighted acquisition contradicts LTV target |

All source code changes have been correctly applied. The two failing checks are data model design decisions, not implementation bugs.
