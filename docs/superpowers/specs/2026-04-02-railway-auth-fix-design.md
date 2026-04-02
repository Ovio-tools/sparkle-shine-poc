# Railway Auth Fix: Centralize OAuth Env Var Fallback

**Date:** 2026-04-02
**Status:** Draft
**Scope:** Fix auth failures on Railway by adding env var bootstrap tier to token_store and fixing three OAuth modules

---

## Problem

On Railway, two auth failures block the automation-runner and intelligence services:

1. **Jobber 401:** Token refresh fails because `.jobber_tokens.json` doesn't exist in the container. The code bootstraps from `JOBBER_REFRESH_TOKEN` env var, but if refresh fails, it hard-errors via `get_credential("JOBBER_ACCESS_TOKEN")` instead of handling gracefully.

2. **Google FileNotFoundError:** `credentials.json` not found at `/app/credentials.json`. The code has env var bootstrap for `GOOGLE_REFRESH_TOKEN` (google_auth.py:97-99), but when it constructs Credentials with only a refresh_token (no access token), `creds.expired` returns `False`, the refresh condition on line 125 (`creds.expired and creds.refresh_token`) fails, and it falls through to the browser consent flow which calls `_credentials_file()` and crashes.

3. **QuickBooks (latent):** Same pattern as Jobber — will hit the same hard-error when its token expires on Railway.

All required env vars (`JOBBER_REFRESH_TOKEN`, `GOOGLE_REFRESH_TOKEN`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `QBO_REFRESH_TOKEN`, etc.) are already set in Railway. The auth code just doesn't reach them correctly.

---

## Design

### Approach: Centralize env var fallback in token_store + fix auth modules

**Chosen over:**
- Approach A (fix modules individually): would leave QuickBooks as a time bomb and keep env var logic scattered
- Approach C (full auth refactor): overkill for this scope

### Change 1: token_store.py — Add env var tier

**Current chain:** DB → JSON file → empty dict
**New chain:** DB → JSON file → env vars → empty dict

Add `_load_from_env(tool_name)` that checks for `{PREFIX}_REFRESH_TOKEN` and `{PREFIX}_ACCESS_TOKEN` env vars. Returns a token dict if found, `None` otherwise.

Env var prefix mapping (most tools use `TOOL_NAME.upper()`, except QuickBooks):

| tool_name | Prefix | Env vars checked |
|-----------|--------|-----------------|
| jobber | JOBBER | JOBBER_REFRESH_TOKEN, JOBBER_ACCESS_TOKEN |
| quickbooks | QBO | QBO_REFRESH_TOKEN, QBO_ACCESS_TOKEN |
| google | GOOGLE | GOOGLE_REFRESH_TOKEN |

**Note on Google client credentials:** `_load_from_env()` only returns token data (refresh_token, access_token) — not OAuth app credentials. Google also needs `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` to construct a `Credentials` object and perform the refresh. These are not tokens, so they do not belong in `_load_from_env()`'s return dict. Instead, `_build_creds_from_dict()` in google_auth.py already pulls them separately via `os.getenv("GOOGLE_CLIENT_ID")` and `os.getenv("GOOGLE_CLIENT_SECRET")` (lines 79-80). No change needed there — the existing code handles it.

The env var tier is **bootstrap-only**: once a successful token refresh writes to PostgreSQL (which already happens via `save_tokens()`), subsequent `load_tokens()` calls return the DB data and never reach the env var tier.

### Change 2: auth/jobber_auth.py — Remove redundant env var logic, fix fallback

**Remove:** Lines 87-89 (env var bootstrap) — token_store handles this now.

**Replace line 106** (`return get_credential("JOBBER_ACCESS_TOKEN")`):
- Try `JOBBER_ACCESS_TOKEN` from env var via `os.getenv()` (not `get_credential()` which hard-errors)
- If available, log a warning and return it as stale last resort
- If not available, raise `RuntimeError` with a clear message explaining what's needed

**Improve refresh error logging (line 102-103):**
- Log at `logger.warning` instead of `print()`
- Include HTTP status and response body excerpt when available (catch `requests.HTTPError` specifically)

### Change 3: auth/google_auth.py — Fix refresh condition, handle missing credentials.json

**Root cause fix (line 124-125):** Change condition from:
```python
if creds and creds.expired and creds.refresh_token:
```
to:
```python
if creds and creds.refresh_token and (not creds.token or creds.expired):
```

This covers both cases: (a) env var bootstrap where there's no access token yet (`not creds.token`), and (b) normal refresh cycle where the access token has expired (`creds.expired`). Without the `not creds.token` check, the original condition skips refresh entirely when bootstrapping from env vars because `expired` returns `False` when there's no token to be expired. Without the `creds.expired` check, the naive fix (`if creds and creds.refresh_token`) would burn a refresh call on every invocation even when the access token is still valid.

**Browser flow fallback (line 128-131):** Wrap `_credentials_file()` in a try/except. If `FileNotFoundError`, raise a clear `RuntimeError` explaining that on Railway you need `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, and `GOOGLE_REFRESH_TOKEN` env vars.

**Remove manual env var bootstrap (lines 97-99):** Replace the three-tier manual loading (env var check, then DB via token_store, then token.json) with a single `token_store.load_tokens("google", token_path)` call. Since token_store now checks DB → JSON file → env vars, this one call covers all three tiers. Pass the result through `_build_creds_from_dict()`.

**Scope check heuristic:** After consolidation, `google_auth` can't tell which tier produced the dict. The scope check (lines 110-122) only matters for token.json data. Resolution: only run the scope check if the dict contains a `"scopes"` key. Token.json (written by the Google library) includes a `"scopes"` key; DB and env var dicts don't. If scopes are present but insufficient, delete token.json and clear token_data so the code falls through to the browser flow (or RuntimeError on Railway).

**Save to DB after refresh:** Already happens on line 137.

**Wrap token.json write in try/except (line 133-134):** The current code writes to `token_path` unconditionally. On Railway, this path may not be writable (read-only filesystem or missing parent directory). Wrap lines 133-134 in `try/except (OSError, PermissionError)`, log at debug level, and continue. The DB write on line 137 is the important one; the JSON file write is a local-dev convenience.

### Change 4: auth/quickbooks_auth.py — Same pattern as Jobber

**Remove:** Lines 93-94 (env var bootstrap) — token_store handles this.

**Replace line 110** (`return get_credential("QBO_ACCESS_TOKEN")`):
- Same graceful fallback pattern as Jobber
- Try `QBO_ACCESS_TOKEN` from `os.getenv()`, warn if used
- Raise clear `RuntimeError` if nothing works

**Improve refresh error logging (line 108):**
- Same as Jobber: replace `print()` with `logger.warning`
- Catch `requests.HTTPError` specifically to log HTTP status and response body excerpt

---

## What does NOT change

- `auth/__init__.py` and `auth.get_client()` public interface — callers unaffected
- JSON file token path — still works for local dev where files exist
- `credentials.py` — untouched
- `poll_state` or automation runner logic — untouched
- Token save flow — already writes to both DB and JSON; no changes needed
- `run_initial_auth()` functions — browser-based flows unchanged

---

## Verification plan

1. **Simulate Railway locally:** Rename `.jobber_tokens.json`, `.quickbooks_tokens.json`, `token.json`, and `credentials.json` to `.bak` extensions. Set env vars. Confirm each `get_client()` call succeeds from env vars alone.

2. **Confirm DB persistence:** After first successful refresh, check `oauth_tokens` table for the tool's row. Confirm subsequent calls read from DB (add debug log or check that env var tier is skipped).

3. **Confirm env vars not consulted after DB write:** Unset the env vars after first successful refresh. Confirm second call still works (reading from DB).

4. **Run existing tests:** `python -m pytest tests/test_automations/ -v` and `python tests/test_phase4.py -v -k "not live and not slack_channel"` to confirm local dev path still works.

5. **Test dead refresh token:** Temporarily set `JOBBER_REFRESH_TOKEN` to an invalid value (e.g., `"revoked"`), remove the DB row for jobber from `oauth_tokens`, and call `get_client("jobber")`. Confirm the error message is clear and actionable (names the tool, says the refresh token is invalid, tells you what to do), not a raw stack trace. Repeat for Google and QuickBooks.

6. **Deploy to Railway:** Redeploy automation-runner, check logs for successful Jobber/Google/QBO auth.

---

## File change summary

| File | Change type | Lines affected |
|------|------------|---------------|
| auth/token_store.py | Add `_load_from_env()`, update `load_tokens()` | ~15 new lines |
| auth/jobber_auth.py | Remove env var bootstrap, fix fallback, improve logging | ~10 lines changed |
| auth/google_auth.py | Fix refresh condition, handle missing credentials.json, use token_store for env vars | ~20 lines changed |
| auth/quickbooks_auth.py | Remove env var bootstrap, fix fallback | ~10 lines changed |
