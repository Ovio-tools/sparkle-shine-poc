# Railway Auth Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix OAuth auth failures on Railway by centralizing env var fallback in token_store and fixing Jobber, Google, and QuickBooks auth modules.

**Architecture:** Add an env var bootstrap tier to `auth/token_store.py` (DB → JSON → env vars → empty dict). Remove redundant env var logic from each auth module. Fix error handling so refresh failures produce clear messages instead of crashes.

**Tech Stack:** Python 3, psycopg2 (PostgreSQL), google-auth, requests

**Spec:** `docs/superpowers/specs/2026-04-02-railway-auth-fix-design.md`

---

## File Map

| File | Role | Action |
|------|------|--------|
| `auth/token_store.py` | Centralized token storage (DB → JSON → env → empty) | Modify: add `_load_from_env()`, update `load_tokens()` |
| `auth/jobber_auth.py` | Jobber OAuth token management | Modify: remove env var bootstrap, fix fallback, improve logging |
| `auth/google_auth.py` | Google OAuth token management | Modify: fix refresh condition, handle missing credentials.json, use token_store for loading |
| `auth/quickbooks_auth.py` | QuickBooks OAuth token management | Modify: remove env var bootstrap, fix fallback |
| `tests/test_auth/test_token_store.py` | Tests for token_store env var tier | Create |
| `tests/test_auth/__init__.py` | Package init | Create |
| `tests/test_auth/test_jobber_auth.py` | Tests for Jobber auth fallback | Create |
| `tests/test_auth/test_google_auth.py` | Tests for Google auth refresh condition | Create |
| `tests/test_auth/test_quickbooks_auth.py` | Tests for QuickBooks auth fallback | Create |

---

### Task 1: Add env var tier to token_store.py

**Files:**
- Modify: `auth/token_store.py:60-73`
- Create: `tests/test_auth/__init__.py`
- Create: `tests/test_auth/test_token_store.py`

- [ ] **Step 1: Create test directory and write failing tests**

Create `tests/test_auth/__init__.py` (empty file).

Create `tests/test_auth/test_token_store.py`:

```python
"""Tests for token_store env var fallback tier."""
import os
import unittest
from unittest.mock import patch, MagicMock

# Patch database.connection before importing token_store
# to avoid requiring a live PostgreSQL connection
import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from auth import token_store


class TestLoadFromEnv(unittest.TestCase):
    """Test the env var fallback tier in token_store."""

    def test_returns_none_when_no_env_vars(self):
        """No env vars set -> returns None."""
        with patch.dict(os.environ, {}, clear=True):
            result = token_store._load_from_env("jobber")
            self.assertIsNone(result)

    def test_returns_refresh_token_from_env(self):
        """JOBBER_REFRESH_TOKEN set -> returns dict with refresh_token."""
        with patch.dict(os.environ, {"JOBBER_REFRESH_TOKEN": "rt_abc123"}, clear=True):
            result = token_store._load_from_env("jobber")
            self.assertEqual(result, {"refresh_token": "rt_abc123"})

    def test_returns_both_tokens_from_env(self):
        """Both JOBBER_REFRESH_TOKEN and JOBBER_ACCESS_TOKEN set."""
        env = {
            "JOBBER_REFRESH_TOKEN": "rt_abc123",
            "JOBBER_ACCESS_TOKEN": "at_xyz789",
        }
        with patch.dict(os.environ, env, clear=True):
            result = token_store._load_from_env("jobber")
            self.assertEqual(result, {
                "refresh_token": "rt_abc123",
                "access_token": "at_xyz789",
            })

    def test_quickbooks_uses_qbo_prefix(self):
        """QuickBooks uses QBO_ prefix, not QUICKBOOKS_."""
        with patch.dict(os.environ, {"QBO_REFRESH_TOKEN": "rt_qbo"}, clear=True):
            result = token_store._load_from_env("quickbooks")
            self.assertEqual(result, {"refresh_token": "rt_qbo"})

    def test_quickbooks_ignores_wrong_prefix(self):
        """QUICKBOOKS_REFRESH_TOKEN should NOT be picked up."""
        with patch.dict(os.environ, {"QUICKBOOKS_REFRESH_TOKEN": "rt_wrong"}, clear=True):
            result = token_store._load_from_env("quickbooks")
            self.assertIsNone(result)

    def test_google_returns_refresh_token(self):
        """GOOGLE_REFRESH_TOKEN set."""
        with patch.dict(os.environ, {"GOOGLE_REFRESH_TOKEN": "rt_google"}, clear=True):
            result = token_store._load_from_env("google")
            self.assertEqual(result, {"refresh_token": "rt_google"})


class TestLoadTokensEnvFallback(unittest.TestCase):
    """Test that load_tokens falls through to env vars when DB and JSON fail."""

    @patch.object(token_store, "_load_from_db", return_value=None)
    def test_env_var_fallback_when_db_and_json_missing(self, mock_db):
        """No DB, no JSON file -> falls through to env var."""
        with patch.dict(os.environ, {"JOBBER_REFRESH_TOKEN": "rt_fallback"}, clear=True):
            result = token_store.load_tokens("jobber", "/nonexistent/path.json")
            self.assertEqual(result, {"refresh_token": "rt_fallback"})

    @patch.object(token_store, "_load_from_db", return_value={"refresh_token": "rt_from_db"})
    def test_db_takes_priority_over_env(self, mock_db):
        """DB has tokens -> env vars are NOT consulted."""
        with patch.dict(os.environ, {"JOBBER_REFRESH_TOKEN": "rt_env"}, clear=True):
            result = token_store.load_tokens("jobber")
            self.assertEqual(result["refresh_token"], "rt_from_db")

    @patch.object(token_store, "_load_from_db", return_value=None)
    def test_returns_empty_dict_when_nothing_available(self, mock_db):
        """No DB, no JSON, no env vars -> returns empty dict."""
        with patch.dict(os.environ, {}, clear=True):
            result = token_store.load_tokens("jobber", "/nonexistent/path.json")
            self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_auth/test_token_store.py -v`

Expected: FAIL — `_load_from_env` does not exist yet.

- [ ] **Step 3: Implement `_load_from_env` and update `load_tokens`**

In `auth/token_store.py`, add the env var prefix mapping and `_load_from_env` function after the existing `_save_to_db` function (after line 57), and update `load_tokens` to call it:

```python
# -- Add after _save_to_db (line 57) --

_ENV_PREFIX_MAP = {
    "quickbooks": "QBO",
}


def _load_from_env(tool_name: str) -> dict | None:
    """Load tokens from environment variables (bootstrap-only fallback)."""
    prefix = _ENV_PREFIX_MAP.get(tool_name, tool_name.upper())
    refresh = os.getenv(f"{prefix}_REFRESH_TOKEN")
    if not refresh:
        return None
    result = {"refresh_token": refresh}
    access = os.getenv(f"{prefix}_ACCESS_TOKEN")
    if access:
        result["access_token"] = access
    logger.debug("[token_store] Bootstrapped %s tokens from env vars", tool_name)
    return result
```

Update the `load_tokens` function to add the env var tier between JSON file and empty dict. Replace the entire function:

```python
def load_tokens(tool_name: str, json_path: str | None = None) -> dict:
    """Load tokens using four-tier fallback: DB -> JSON file -> env vars -> empty dict."""
    data = _load_from_db(tool_name)
    if data:
        return data

    if json_path and os.path.exists(json_path):
        try:
            with open(json_path) as f:
                return json.load(f)
        except Exception as exc:
            logger.debug("[token_store] JSON load failed for %s: %s", json_path, exc)

    data = _load_from_env(tool_name)
    if data:
        return data

    return {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_auth/test_token_store.py -v`

Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add auth/token_store.py tests/test_auth/__init__.py tests/test_auth/test_token_store.py
git commit -m "feat(auth): add env var fallback tier to token_store

token_store.load_tokens now checks DB -> JSON file -> env vars -> empty dict.
The env var tier is bootstrap-only: after first successful refresh, tokens
persist to PostgreSQL and env vars are never consulted again."
```

---

### Task 2: Fix Jobber auth fallback and remove redundant env var logic

**Files:**
- Modify: `auth/jobber_auth.py:80-106`
- Create: `tests/test_auth/test_jobber_auth.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_auth/test_jobber_auth.py`:

```python
"""Tests for Jobber auth fallback behavior."""
import os
import unittest
from unittest.mock import patch, MagicMock

import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from auth import jobber_auth


class TestGetJobberTokenFallback(unittest.TestCase):
    """Test graceful fallback when token refresh fails."""

    @patch.object(jobber_auth, "_load_tokens", return_value={})
    def test_raises_runtime_error_when_no_tokens(self, mock_load):
        """No tokens anywhere -> clear RuntimeError, not EnvironmentError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                jobber_auth.get_jobber_token()
            self.assertIn("Jobber", str(ctx.exception))
            self.assertIn("JOBBER_REFRESH_TOKEN", str(ctx.exception))

    @patch.object(jobber_auth, "_load_tokens", return_value={"refresh_token": "dead_token"})
    @patch.object(jobber_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_falls_back_to_access_token_env_var(self, mock_refresh, mock_load):
        """Refresh fails + JOBBER_ACCESS_TOKEN set -> returns it with warning."""
        with patch.dict(os.environ, {"JOBBER_ACCESS_TOKEN": "at_stale"}, clear=True):
            token = jobber_auth.get_jobber_token()
            self.assertEqual(token, "at_stale")

    @patch.object(jobber_auth, "_load_tokens", return_value={"refresh_token": "dead_token"})
    @patch.object(jobber_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_raises_clear_error_when_refresh_fails_no_fallback(self, mock_refresh, mock_load):
        """Refresh fails + no JOBBER_ACCESS_TOKEN -> clear RuntimeError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                jobber_auth.get_jobber_token()
            self.assertIn("refresh", str(ctx.exception).lower())

    @patch.object(jobber_auth, "_load_tokens", return_value={
        "access_token": "at_valid", "expires_at": 9999999999, "refresh_token": "rt_ok"
    })
    def test_returns_valid_unexpired_token(self, mock_load):
        """Valid unexpired token -> returns it without refresh."""
        token = jobber_auth.get_jobber_token()
        self.assertEqual(token, "at_valid")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_auth/test_jobber_auth.py -v`

Expected: `test_raises_runtime_error_when_no_tokens` FAILS (currently raises `EnvironmentError` from `get_credential`, not `RuntimeError`). `test_raises_clear_error_when_refresh_fails_no_fallback` FAILS (same reason).

- [ ] **Step 3: Fix `get_jobber_token` in jobber_auth.py**

Add a logger import at the top of `auth/jobber_auth.py` (after the existing imports, around line 20):

```python
import logging

logger = logging.getLogger(__name__)
```

Replace the `get_jobber_token` function (lines 80-106) with:

```python
def get_jobber_token() -> str:
    """
    Return a valid Jobber access token.
    Order: DB/file/env (via token_store) → auto-refresh → stale env-var fallback.
    """
    tokens = _load_tokens()

    if tokens.get("access_token"):
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - _EXPIRY_BUFFER:
            return tokens["access_token"]

    # Try to refresh if we have a refresh token
    if tokens.get("refresh_token"):
        try:
            new_tokens = _refresh_token(tokens["refresh_token"])
            _save_tokens(new_tokens)
            return new_tokens["access_token"]
        except requests.HTTPError as exc:
            logger.warning(
                "[jobber] Token refresh failed: HTTP %s — %s",
                exc.response.status_code if exc.response is not None else "?",
                exc.response.text[:300] if exc.response is not None else str(exc),
            )
        except Exception as exc:
            logger.warning("[jobber] Token refresh failed: %s", exc)

    # Last resort: stale access token from env var
    stale_token = os.getenv("JOBBER_ACCESS_TOKEN")
    if stale_token:
        logger.warning(
            "[jobber] Using JOBBER_ACCESS_TOKEN env var as last resort (may be stale)"
        )
        return stale_token

    raise RuntimeError(
        "No valid Jobber token available. "
        "Set JOBBER_REFRESH_TOKEN (plus JOBBER_CLIENT_ID and JOBBER_CLIENT_SECRET) "
        "in environment variables to enable token refresh. "
        "Or provide JOBBER_ACCESS_TOKEN as a temporary fallback."
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_auth/test_jobber_auth.py -v`

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add auth/jobber_auth.py tests/test_auth/test_jobber_auth.py
git commit -m "fix(auth): Jobber auth graceful fallback on refresh failure

Remove redundant env var bootstrap (token_store handles it).
Replace hard get_credential() fallback with clear RuntimeError.
Log HTTP status and response body on refresh failure."
```

---

### Task 3: Fix Google auth refresh condition and missing credentials.json handling

**Files:**
- Modify: `auth/google_auth.py:87-139`
- Create: `tests/test_auth/test_google_auth.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_auth/test_google_auth.py`:

```python
"""Tests for Google auth refresh condition and Railway fallback."""
import os
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from google.oauth2.credentials import Credentials

from auth import google_auth


class TestRefreshCondition(unittest.TestCase):
    """Test the refresh condition covers both bootstrap and normal paths."""

    @patch.object(google_auth, "token_store")
    @patch("auth.google_auth.Request")
    def test_refreshes_when_no_access_token_but_has_refresh(self, mock_request, mock_store):
        """Env var bootstrap: refresh_token present, no access token -> should refresh."""
        mock_store.load_tokens.return_value = {"refresh_token": "rt_env"}
        mock_store.save_tokens = MagicMock()

        # Create a real Credentials object with just a refresh token
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.token = None
        mock_creds.valid = False
        mock_creds.expired = False  # No token to be expired
        mock_creds.refresh_token = "rt_env"

        with patch.object(google_auth, "_build_creds_from_dict", return_value=mock_creds):
            with patch.object(mock_creds, "to_json", return_value='{"token": "new", "refresh_token": "rt_env"}'):
                google_auth.get_google_credentials()

        # Should have called refresh, not the browser flow
        mock_creds.refresh.assert_called_once()

    @patch.object(google_auth, "token_store")
    def test_skips_refresh_when_token_valid(self, mock_store):
        """Valid unexpired token -> no refresh, return immediately."""
        mock_store.load_tokens.return_value = {"refresh_token": "rt", "token": "at_valid"}
        mock_store.save_tokens = MagicMock()

        mock_creds = MagicMock(spec=Credentials)
        mock_creds.token = "at_valid"
        mock_creds.valid = True
        mock_creds.expired = False
        mock_creds.refresh_token = "rt"

        with patch.object(google_auth, "_build_creds_from_dict", return_value=mock_creds):
            result = google_auth.get_google_credentials()

        mock_creds.refresh.assert_not_called()
        self.assertEqual(result, mock_creds)


class TestMissingCredentialsJson(unittest.TestCase):
    """Test that missing credentials.json raises clear error, not FileNotFoundError."""

    @patch.object(google_auth, "token_store")
    @patch("auth.google_auth.Request")
    def test_clear_error_when_no_credentials_json_and_no_env(self, mock_request, mock_store):
        """No credentials.json, no env vars -> RuntimeError with guidance."""
        mock_store.load_tokens.return_value = {}

        with patch.object(google_auth, "_build_creds_from_dict", return_value=None):
            with self.assertRaises(RuntimeError) as ctx:
                google_auth.get_google_credentials()
            self.assertIn("GOOGLE_REFRESH_TOKEN", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_auth/test_google_auth.py -v`

Expected: `test_refreshes_when_no_access_token_but_has_refresh` FAILS (current code doesn't refresh when `expired` is False). `test_clear_error_when_no_credentials_json_and_no_env` FAILS (raises `FileNotFoundError`, not `RuntimeError`).

- [ ] **Step 3: Rewrite `get_google_credentials` in google_auth.py**

Add a logger import at the top of `auth/google_auth.py` (after the existing imports, around line 23):

```python
import logging

logger = logging.getLogger(__name__)
```

Replace the `get_google_credentials` function (lines 87-139) with:

```python
def get_google_credentials() -> Credentials:
    """
    Return valid Google Credentials.
    Order: DB -> JSON file -> env vars (all via token_store) -> browser consent flow.
    After any refresh, updated credentials are saved to DB.
    """
    token_path = _token_file()
    creds = None

    # Load tokens via token_store (DB -> JSON -> env vars)
    token_data = token_store.load_tokens(
        "google", token_path if os.path.exists(token_path) else None
    )

    # Scope check: only applies when data came from token.json (includes "scopes" key).
    # DB and env var dicts don't have a "scopes" key, so this is skipped for them.
    if token_data.get("refresh_token") and "scopes" in token_data:
        _stored_raw = token_data["scopes"]
        if isinstance(_stored_raw, list):
            _stored_scopes = set(_stored_raw)
        else:
            _stored_scopes = set(str(_stored_raw).split())
        if not all(s in _stored_scopes for s in _SCOPES):
            # Scopes insufficient — delete stale token.json, fall through to browser flow
            if os.path.exists(token_path):
                os.remove(token_path)
            token_data = {}

    if token_data.get("refresh_token"):
        creds = _build_creds_from_dict(token_data)

    if not creds or not creds.valid:
        if creds and creds.refresh_token and (not creds.token or creds.expired):
            creds.refresh(Request())

            # Save refreshed credentials to token.json (local dev convenience)
            try:
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            except (OSError, PermissionError):
                logger.debug("[google] Could not write token.json (read-only filesystem?)")

            # Save refreshed credentials to DB (primary persistence)
            token_store.save_tokens("google", json.loads(creds.to_json()))
        else:
            # Browser consent flow — only works locally with credentials.json
            try:
                creds_file = _credentials_file()
            except FileNotFoundError:
                raise RuntimeError(
                    "No valid Google credentials available. "
                    "Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and GOOGLE_REFRESH_TOKEN "
                    "as environment variables (for Railway/CI), or provide credentials.json "
                    "for the browser consent flow (local dev)."
                )
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, _SCOPES)
            creds = flow.run_local_server(port=8025, open_browser=True)

            try:
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            except (OSError, PermissionError):
                logger.debug("[google] Could not write token.json (read-only filesystem?)")

            token_store.save_tokens("google", json.loads(creds.to_json()))

    return creds
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_auth/test_google_auth.py -v`

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add auth/google_auth.py tests/test_auth/test_google_auth.py
git commit -m "fix(auth): Google auth refresh condition and missing credentials.json

Fix condition to refresh when bootstrapped from env vars (no access token
yet but refresh_token present). Wrap credentials.json lookup in try/except
to raise clear RuntimeError on Railway instead of FileNotFoundError.
Wrap token.json writes in try/except for read-only filesystems."
```

---

### Task 4: Fix QuickBooks auth fallback

**Files:**
- Modify: `auth/quickbooks_auth.py:85-110`
- Create: `tests/test_auth/test_quickbooks_auth.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_auth/test_quickbooks_auth.py`:

```python
"""Tests for QuickBooks auth fallback behavior."""
import os
import unittest
from unittest.mock import patch, MagicMock

import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from auth import quickbooks_auth


class TestGetQuickbooksTokenFallback(unittest.TestCase):
    """Test graceful fallback when token refresh fails."""

    @patch.object(quickbooks_auth, "_load_tokens", return_value={})
    def test_raises_runtime_error_when_no_tokens(self, mock_load):
        """No tokens anywhere -> clear RuntimeError, not EnvironmentError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                quickbooks_auth.get_quickbooks_token()
            self.assertIn("QuickBooks", str(ctx.exception))
            self.assertIn("QBO_REFRESH_TOKEN", str(ctx.exception))

    @patch.object(quickbooks_auth, "_load_tokens", return_value={"refresh_token": "dead"})
    @patch.object(quickbooks_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_falls_back_to_access_token_env_var(self, mock_refresh, mock_load):
        """Refresh fails + QBO_ACCESS_TOKEN set -> returns it with warning."""
        with patch.dict(os.environ, {"QBO_ACCESS_TOKEN": "at_stale"}, clear=True):
            token = quickbooks_auth.get_quickbooks_token()
            self.assertEqual(token, "at_stale")

    @patch.object(quickbooks_auth, "_load_tokens", return_value={"refresh_token": "dead"})
    @patch.object(quickbooks_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_raises_clear_error_when_refresh_fails_no_fallback(self, mock_refresh, mock_load):
        """Refresh fails + no QBO_ACCESS_TOKEN -> clear RuntimeError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                quickbooks_auth.get_quickbooks_token()
            self.assertIn("refresh", str(ctx.exception).lower())

    @patch.object(quickbooks_auth, "_load_tokens", return_value={
        "access_token": "at_valid", "expires_at": 9999999999, "refresh_token": "rt_ok"
    })
    def test_returns_valid_unexpired_token(self, mock_load):
        """Valid unexpired token -> returns it without refresh."""
        token = quickbooks_auth.get_quickbooks_token()
        self.assertEqual(token, "at_valid")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_auth/test_quickbooks_auth.py -v`

Expected: `test_raises_runtime_error_when_no_tokens` FAILS (raises `EnvironmentError`). `test_raises_clear_error_when_refresh_fails_no_fallback` FAILS (same).

- [ ] **Step 3: Fix `get_quickbooks_token` in quickbooks_auth.py**

Add a logger import at the top of `auth/quickbooks_auth.py` (after the existing imports, around line 20):

```python
import logging

logger = logging.getLogger(__name__)
```

Replace the `get_quickbooks_token` function (lines 85-110) with:

```python
def get_quickbooks_token() -> str:
    """
    Return a valid QBO access token.
    Order: DB/file/env (via token_store) → auto-refresh → stale env-var fallback.
    """
    tokens = _load_tokens()

    if tokens.get("access_token"):
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - _EXPIRY_BUFFER:
            return tokens["access_token"]

    # Try to refresh if we have a refresh token
    if tokens.get("refresh_token"):
        try:
            new_tokens = _refresh_token(tokens["refresh_token"])
            _save_tokens(new_tokens)
            return new_tokens["access_token"]
        except requests.HTTPError as exc:
            logger.warning(
                "[quickbooks] Token refresh failed: HTTP %s — %s",
                exc.response.status_code if exc.response is not None else "?",
                exc.response.text[:300] if exc.response is not None else str(exc),
            )
        except Exception as exc:
            logger.warning("[quickbooks] Token refresh failed: %s", exc)

    # Last resort: stale access token from env var
    stale_token = os.getenv("QBO_ACCESS_TOKEN")
    if stale_token:
        logger.warning(
            "[quickbooks] Using QBO_ACCESS_TOKEN env var as last resort (may be stale)"
        )
        return stale_token

    raise RuntimeError(
        "No valid QuickBooks token available. "
        "Set QBO_REFRESH_TOKEN (plus QUICKBOOKS_CLIENT_ID and QUICKBOOKS_CLIENT_SECRET) "
        "in environment variables to enable token refresh. "
        "Or provide QBO_ACCESS_TOKEN as a temporary fallback."
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_auth/test_quickbooks_auth.py -v`

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add auth/quickbooks_auth.py tests/test_auth/test_quickbooks_auth.py
git commit -m "fix(auth): QuickBooks auth graceful fallback on refresh failure

Same pattern as Jobber fix: remove redundant env var bootstrap,
replace hard get_credential() fallback with clear RuntimeError,
log HTTP details on refresh failure."
```

---

### Task 5: Run full test suite and verify local dev path

**Files:**
- None (verification only)

- [ ] **Step 1: Run all new auth tests**

Run: `python -m pytest tests/test_auth/ -v`

Expected: All 20 tests PASS.

- [ ] **Step 2: Run existing automation tests**

Run: `python -m pytest tests/test_automations/ -v`

Expected: All existing tests PASS (no regressions).

- [ ] **Step 3: Run phase 4 tests**

Run: `python tests/test_phase4.py -v -k "not live and not slack_channel"`

Expected: All tests PASS.

- [ ] **Step 4: Commit the spec**

```bash
git add docs/superpowers/specs/2026-04-02-railway-auth-fix-design.md docs/superpowers/plans/2026-04-02-railway-auth-fix.md
git commit -m "docs: add Railway auth fix design spec and implementation plan"
```
