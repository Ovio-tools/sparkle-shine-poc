"""
Phase 1 integration tests for Sparkle & Shine POC.

All tests make real API calls and real DB queries.
Run with:
    pytest tests/test_phase1.py -v --tb=short
    python tests/test_phase1.py          # uses __main__ block below
"""

import json
import os
import sys

import pytest
import requests

# ------------------------------------------------------------------ #
# Path setup (also done in conftest, but kept here for direct runs)
# ------------------------------------------------------------------ #
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from tests.conftest import requires_google  # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
_TOOL_IDS_PATH = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")

_EXPECTED_TABLES = [
    "clients", "leads", "employees", "crews", "jobs",
    "recurring_agreements", "commercial_proposals", "invoices", "payments",
    "marketing_campaigns", "marketing_interactions", "reviews", "tasks",
    "calendar_events", "documents", "cross_tool_mapping",
    "daily_metrics_snapshot", "document_index",
]

_EXPECTED_DOC_KEYS = [
    "employee_handbook",
    "service_quality_checklist",
    "client_onboarding_guide",
    "safety_chemical_manual",
    "sales_proposal_templates",
    "marketing_playbook",
    "vendor_supplier_directory",
    "fy2026_growth_plan",
]

_EXPECTED_SHEET_KEYS = [
    "rate_card",
    "budget_tracker",
    "supply_inventory",
    "vehicle_equipment_log",
]


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #
def _load_tool_ids() -> dict:
    with open(_TOOL_IDS_PATH) as f:
        return json.load(f)


def _pd_url(session) -> str:
    """Return a normalised Pipedrive base URL from the session's stored attribute."""
    base = getattr(session, "base_url", "https://api.pipedrive.com/v1").rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"
    return base


# ------------------------------------------------------------------ #
# Test class
# ------------------------------------------------------------------ #
class TestPhase1Integration:

    # ---------------------------------------------------------------- #
    # 1. Environment
    # ---------------------------------------------------------------- #
    def test_env_credentials_complete(self):
        """All required credentials are present, including one valid Google auth mode."""
        from credentials import missing_required_credentials
        missing = missing_required_credentials()
        assert not missing, f"Missing credentials: {missing}"

    # ---------------------------------------------------------------- #
    # 2. Database schema
    # ---------------------------------------------------------------- #
    def test_database_schema_complete(self):
        """PostgreSQL database contains all expected tables."""
        from database.schema import get_connection
        conn = get_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        ).fetchall()
        conn.close()

        actual = {r["table_name"] for r in rows}
        missing = set(_EXPECTED_TABLES) - actual
        assert not missing, f"Missing tables: {missing}"
        assert len(actual) >= 18, f"Expected ≥18 tables, found {len(actual)}"

    # ---------------------------------------------------------------- #
    # 3. Cross-tool mapping roundtrip
    # ---------------------------------------------------------------- #
    def test_cross_tool_mapping_roundtrip(self, test_db_path):
        """Generate a canonical ID, register / retrieve / reverse-lookup a mapping."""
        from database.schema import init_db
        from database.mappings import (
            generate_id, register_mapping,
            get_tool_id, get_canonical_id,
        )

        init_db(test_db_path)

        canonical_id = generate_id("CLIENT", db_path=test_db_path)
        assert canonical_id.startswith("SS-CLIENT-"), \
            f"Unexpected ID format: {canonical_id}"

        fake_tool = "test_tool"
        fake_tool_id = f"FAKE-{canonical_id}"
        fake_url = "https://example.com/test/FAKE-XYZ-9999"

        register_mapping(canonical_id, fake_tool, fake_tool_id, fake_url,
                         db_path=test_db_path)

        # Forward lookup
        retrieved = get_tool_id(canonical_id, fake_tool, db_path=test_db_path)
        assert retrieved == fake_tool_id, \
            f"Forward lookup mismatch: expected {fake_tool_id}, got {retrieved}"

        # Reverse lookup
        resolved = get_canonical_id(fake_tool, fake_tool_id, db_path=test_db_path)
        assert resolved == canonical_id, \
            f"Reverse lookup mismatch: expected {canonical_id}, got {resolved}"

    # ---------------------------------------------------------------- #
    # 4. Pipedrive auth
    # ---------------------------------------------------------------- #
    def test_auth_pipedrive(self):
        """Pipedrive session can GET /users/me and receives 200 with a 'data' key."""
        from auth import get_client
        session = get_client("pipedrive")

        base = _pd_url(session)
        resp = session.get(f"{base}/users/me", timeout=15)

        assert resp.status_code == 200, \
            f"Expected 200, got {resp.status_code}: {resp.text[:200]}"
        body = resp.json()
        assert "data" in body, f"'data' key missing from response: {list(body)}"
        assert body["data"] is not None

    # ---------------------------------------------------------------- #
    # 5. Asana auth
    # ---------------------------------------------------------------- #
    def test_auth_asana(self):
        """Asana client can fetch the current user and returns a valid GID."""
        import asana
        from auth import get_client

        client = get_client("asana")
        users_api = asana.UsersApi(client)
        user = users_api.get_user("me", opts={})

        assert user is not None
        # The SDK returns a dict-like object; gid is accessible as a key or attribute
        gid = user.get("gid") if isinstance(user, dict) else getattr(user, "gid", None)
        assert gid, f"Expected a non-empty user GID, got: {gid}"

    # ---------------------------------------------------------------- #
    # 6. HubSpot auth
    # ---------------------------------------------------------------- #
    def test_auth_hubspot(self):
        """HubSpot client can call the CRM owners API without error."""
        from auth import get_client

        client = get_client("hubspot")
        result = client.crm.owners.owners_api.get_page(limit=1)
        assert result is not None, "Expected a non-None response from HubSpot owners API"
        assert hasattr(result, "results"), \
            f"Response missing 'results' attribute: {dir(result)}"

    # ---------------------------------------------------------------- #
    # 7. Mailchimp auth
    # ---------------------------------------------------------------- #
    def test_auth_mailchimp(self):
        """Mailchimp client ping returns a recognised health status."""
        from auth import get_client

        client = get_client("mailchimp")
        result = client.ping.get()

        assert result is not None
        assert "health_status" in result, \
            f"Expected 'health_status' in ping response, got: {result}"

    # ---------------------------------------------------------------- #
    # 8. Slack auth
    # ---------------------------------------------------------------- #
    def test_auth_slack(self):
        """Slack auth.test returns team name and bot user ID."""
        from auth import get_client

        client = get_client("slack")
        resp = client.auth_test()

        assert resp["ok"] is True, f"Slack auth.test failed: {resp}"
        assert resp.get("team"), "Expected a non-empty team name"
        assert resp.get("bot_id") or resp.get("user_id"), \
            "Expected bot_id or user_id in auth.test response"

    # ---------------------------------------------------------------- #
    # 9. Jobber auth
    # ---------------------------------------------------------------- #
    def test_auth_jobber(self):
        """Jobber session has a valid, non-empty bearer token."""
        from auth import get_client
        from auth.jobber_auth import get_jobber_token

        session = get_client("jobber")
        token = get_jobber_token()

        assert isinstance(token, str) and token.strip(), \
            "Expected a non-empty Jobber token string"
        auth_header = session.headers.get("Authorization", "")
        assert auth_header.startswith("Bearer "), \
            f"Expected 'Bearer ...' Authorization header, got: {auth_header!r}"

    # ---------------------------------------------------------------- #
    # 10. QuickBooks auth
    # ---------------------------------------------------------------- #
    def test_auth_quickbooks(self):
        """QuickBooks headers are valid; company info endpoint returns 200."""
        from auth import get_client
        from auth.quickbooks_auth import get_base_url, get_company_id

        headers = get_client("quickbooks")
        assert "Authorization" in headers, "Missing Authorization header"
        assert headers["Authorization"].startswith("Bearer "), \
            f"Unexpected auth header: {headers['Authorization']!r}"

        company_id = get_company_id()
        url = f"{get_base_url()}/companyinfo/{company_id}"
        resp = requests.get(url, headers=headers, timeout=15)

        assert resp.status_code == 200, \
            f"Expected 200 from QBO companyinfo, got {resp.status_code}: {resp.text[:200]}"
        body = resp.json()
        assert "CompanyInfo" in body or "QueryResponse" in body, \
            f"Unexpected QBO response shape: {list(body)}"

    # ---------------------------------------------------------------- #
    # 11. Google Drive auth
    # ---------------------------------------------------------------- #
    @requires_google
    def test_auth_google_drive(self):
        """Drive service can list files without error (even if the list is empty)."""
        from auth.google_auth import get_drive_service

        drive = get_drive_service()
        result = drive.files().list(pageSize=10, fields="files(id,name)").execute()

        assert "files" in result, \
            f"Expected 'files' key in Drive list response: {list(result)}"
        assert isinstance(result["files"], list)

    # ---------------------------------------------------------------- #
    # 12. tool_ids.json populated
    # ---------------------------------------------------------------- #
    def test_tool_ids_populated(self):
        """tool_ids.json has all expected sections with non-empty IDs."""
        assert os.path.exists(_TOOL_IDS_PATH), \
            f"tool_ids.json not found at {_TOOL_IDS_PATH}"

        ids = _load_tool_ids()

        # Pipedrive: pipelines + stages
        pd = ids.get("pipedrive", {})
        assert pd.get("pipelines"), "Pipedrive pipelines section is empty"
        assert pd.get("stages") and len(pd["stages"]) >= 5, \
            f"Expected ≥5 Pipedrive stages, got: {pd.get('stages')}"

        # Asana: 4 project GIDs
        asana_projects = ids.get("asana", {}).get("projects", {})
        assert len(asana_projects) >= 4, \
            f"Expected 4 Asana projects, got {len(asana_projects)}"

        # Mailchimp: audience_id
        mc = ids.get("mailchimp", {})
        assert mc.get("audience_id"), "Mailchimp audience_id is missing"

        # QuickBooks: ≥5 item IDs
        qb_items = ids.get("quickbooks", {}).get("items", {})
        assert len(qb_items) >= 5, \
            f"Expected ≥5 QuickBooks items, got {len(qb_items)}"

        # Slack: 5 channel entries
        slack_channels = ids.get("slack", {}).get("channels", {})
        assert len(slack_channels) >= 5, \
            f"Expected 5 Slack channel entries, got {len(slack_channels)}"

        # Google: ≥8 doc file IDs
        google_docs = ids.get("google", {}).get("docs", {})
        assert len(google_docs) >= 8, \
            f"Expected ≥8 Google Doc IDs, got {len(google_docs)}"

    # ---------------------------------------------------------------- #
    # 13. Config imports
    # ---------------------------------------------------------------- #
    def test_config_imports(self):
        """Business config imports correctly with exactly 18 employees and 4 crews."""
        from config.business import COMPANY, EMPLOYEES, CREWS, SERVICE_TYPES

        assert COMPANY.get("name"), "COMPANY['name'] is empty"
        assert COMPANY.get("owner_name"), "COMPANY['owner_name'] is empty"

        assert len(EMPLOYEES) == 18, \
            f"Expected 18 employees, got {len(EMPLOYEES)}"
        assert len(CREWS) == 4, \
            f"Expected 4 crews, got {len(CREWS)}"
        assert len(SERVICE_TYPES) >= 7, \
            f"Expected ≥7 service types, got {len(SERVICE_TYPES)}"

        # Spot-check structure
        emp = EMPLOYEES[0]
        assert "id" in emp and emp["id"].startswith("SS-EMP-"), \
            f"Employee ID format unexpected: {emp.get('id')}"

    # ---------------------------------------------------------------- #
    # 14. Google Workspace content
    # ---------------------------------------------------------------- #
    @requires_google
    def test_google_workspace_content(self):
        """Drive contains the 8 expected Docs and 4 expected Sheets from Step 7."""
        from auth.google_auth import get_drive_service

        ids = _load_tool_ids()
        google = ids.get("google", {})
        doc_ids = google.get("docs", {})
        sheet_ids = google.get("sheets", {})

        assert len(doc_ids) >= 8, \
            f"Expected ≥8 doc entries in tool_ids.json, got {len(doc_ids)}"
        assert len(sheet_ids) >= 4, \
            f"Expected ≥4 sheet entries in tool_ids.json, got {len(sheet_ids)}"

        drive = get_drive_service()
        doc_mime = "application/vnd.google-apps.document"
        sheet_mime = "application/vnd.google-apps.spreadsheet"

        # Spot-check first 3 Docs
        for key in _EXPECTED_DOC_KEYS[:3]:
            fid = doc_ids.get(key)
            assert fid, f"No file ID for doc key '{key}' in tool_ids.json"
            meta = drive.files().get(
                fileId=fid, fields="id,name,mimeType"
            ).execute()
            assert meta["mimeType"] == doc_mime, \
                f"'{key}' ({fid}) has unexpected MIME type: {meta['mimeType']}"

        # Spot-check first 2 Sheets
        for key in _EXPECTED_SHEET_KEYS[:2]:
            fid = sheet_ids.get(key)
            assert fid, f"No file ID for sheet key '{key}' in tool_ids.json"
            meta = drive.files().get(
                fileId=fid, fields="id,name,mimeType"
            ).execute()
            assert meta["mimeType"] == sheet_mime, \
                f"'{key}' ({fid}) has unexpected MIME type: {meta['mimeType']}"

    # ---------------------------------------------------------------- #
    # 15. Document index populated
    # ---------------------------------------------------------------- #
    def test_document_index_populated(self):
        """document_index has ≥8 rows; every row has non-empty chunk_text and keywords."""
        from database.schema import get_connection

        conn = get_connection()
        rows = conn.execute("SELECT * FROM document_index").fetchall()
        conn.close()

        if len(rows) == 0:
            pytest.skip("document_index is empty — run Phase 4 document indexing first")

        assert len(rows) >= 8, \
            f"Expected ≥8 rows in document_index, found {len(rows)}"

        empty_text = [r["source_title"] for r in rows if not (r["chunk_text"] or "").strip()]
        empty_kw   = [r["source_title"] for r in rows if not (r["keywords"] or "").strip()]

        assert not empty_text, \
            f"Rows with empty chunk_text (source_title): {empty_text[:5]}"
        assert not empty_kw, \
            f"Rows with empty keywords (source_title): {empty_kw[:5]}"


# ------------------------------------------------------------------ #
# __main__ entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    import subprocess
    result = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            __file__,
            "-v", "--tb=short",
        ],
        cwd=_PROJECT_ROOT,
    )
    sys.exit(result.returncode)
