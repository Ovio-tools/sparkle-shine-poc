"""
setup/configure_tools.py

Idempotent structural setup across all Sparkle & Shine tools.
Run once (or re-run safely) to provision pipelines, properties,
projects, channels, items, etc. Writes all returned IDs to
config/tool_ids.json.

Usage:
    python -m setup.configure_tools
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from auth import get_client
from credentials import get_credential

TOOL_IDS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "config", "tool_ids.json"
)

_CREATED = []   # accumulates "(tool) thing: name" lines for the final summary
_SKIPPED = []   # accumulates "(tool) already exists: name" lines


def _mark(verb: str, tool: str, label: str) -> None:
    line = f"  [{tool}] {verb}: {label}"
    if verb == "created":
        _CREATED.append(line)
    else:
        _SKIPPED.append(line)


def _load_tool_ids() -> dict:
    if os.path.exists(TOOL_IDS_PATH):
        with open(TOOL_IDS_PATH) as f:
            return json.load(f)
    return {}


def _save_tool_ids(data: dict) -> None:
    with open(TOOL_IDS_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ================================================================== #
# PIPEDRIVE
# ================================================================== #

_PD_PIPELINES = {
    "Cleaning Services Sales": [
        "New Lead", "Qualified", "Site Visit Scheduled",
        "Proposal Sent", "Negotiation", "Closed Won",
    ],
    "Lost/Inactive": ["Closed Lost"],
}

_PD_DEAL_FIELDS = [
    {"name": "Client Type",              "field_type": "enum",   "options": ["residential", "commercial"]},
    {"name": "Service Type",             "field_type": "varchar"},
    {"name": "Estimated Monthly Value",  "field_type": "double"},
    {"name": "Lead Source",              "field_type": "varchar"},
]

_PD_PERSON_FIELDS = [
    {"name": "HubSpot Contact ID",  "field_type": "varchar"},
    {"name": "Jobber Client ID",    "field_type": "varchar"},
    {"name": "Acquisition Source",  "field_type": "varchar"},
    {"name": "Neighborhood",        "field_type": "varchar"},
]


def _pd_base(session) -> str:
    raw = getattr(session, "base_url", "https://api.pipedrive.com/v1")
    base = raw.rstrip("/")
    if not any(s in base for s in ("/v1", "/v2")):
        base = f"{base}/v1"
    return base


def _pd_get(session, url: str) -> list:
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data") or []


def _pd_post(session, url: str, payload: dict) -> dict:
    resp = session.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", {})


def configure_pipedrive() -> dict:
    print("\n── Pipedrive ──────────────────────────────────────────")
    session = get_client("pipedrive")
    base = _pd_base(session)
    result = {"pipelines": {}, "stages": {}, "deal_fields": {}, "person_fields": {}}

    # ── Pipelines + Stages ──────────────────────────────────────────
    existing_pipelines = {p["name"]: p for p in _pd_get(session, f"{base}/pipelines")}

    for pipeline_name, stage_names in _PD_PIPELINES.items():
        if pipeline_name in existing_pipelines:
            pipeline_id = existing_pipelines[pipeline_name]["id"]
            _mark("exists", "pipedrive", f"pipeline '{pipeline_name}'")
        else:
            data = _pd_post(session, f"{base}/pipelines", {"name": pipeline_name, "active": True})
            pipeline_id = data["id"]
            _mark("created", "pipedrive", f"pipeline '{pipeline_name}'")

        result["pipelines"][pipeline_name] = pipeline_id

        existing_stages = {
            s["name"]: s
            for s in _pd_get(session, f"{base}/stages?pipeline_id={pipeline_id}")
        }
        for order, stage_name in enumerate(stage_names, 1):
            if stage_name in existing_stages:
                stage_id = existing_stages[stage_name]["id"]
                _mark("exists", "pipedrive", f"  stage '{stage_name}'")
            else:
                data = _pd_post(session, f"{base}/stages", {
                    "name": stage_name,
                    "pipeline_id": pipeline_id,
                    "order_nr": order,
                })
                stage_id = data["id"]
                _mark("created", "pipedrive", f"  stage '{stage_name}'")
            result["stages"][stage_name] = stage_id

    # ── Custom Fields ───────────────────────────────────────────────
    for endpoint, fields, result_key in [
        ("dealFields",   _PD_DEAL_FIELDS,   "deal_fields"),
        ("personFields", _PD_PERSON_FIELDS, "person_fields"),
    ]:
        raw = _pd_get(session, f"{base}/{endpoint}?start=0&limit=200")
        existing_fields = {f["name"]: f for f in raw if f.get("edit_flag")}

        for field_def in fields:
            name = field_def["name"]
            if name in existing_fields:
                key = existing_fields[name]["key"]
                _mark("exists", "pipedrive", f"  field '{name}'")
            else:
                payload: dict = {"name": name, "field_type": field_def["field_type"]}
                if "options" in field_def:
                    payload["options"] = [{"label": o} for o in field_def["options"]]
                data = _pd_post(session, f"{base}/{endpoint}", payload)
                key = data["key"]
                _mark("created", "pipedrive", f"  field '{name}'")
            result[result_key][name] = key

    return result


# ================================================================== #
# HUBSPOT
# ================================================================== #

_HS_CONTACT_PROPS = [
    {"name": "client_type",         "label": "Client Type",          "type": "enumeration",
     "field_type": "select",
     "options": [("Residential", "residential"), ("Commercial", "commercial")]},
    {"name": "service_frequency",   "label": "Service Frequency",    "type": "enumeration",
     "field_type": "select",
     "options": [("Weekly", "weekly"), ("Biweekly", "biweekly"),
                 ("Monthly", "monthly"), ("One-Time", "one-time")]},
    {"name": "lead_source_detail",  "label": "Lead Source Detail",   "type": "string",  "field_type": "text"},
    {"name": "neighborhood",        "label": "Neighborhood",         "type": "string",  "field_type": "text"},
    {"name": "jobber_client_id",    "label": "Jobber Client ID",     "type": "string",  "field_type": "text"},
    {"name": "quickbooks_customer_id", "label": "QuickBooks Customer ID", "type": "string", "field_type": "text"},
    {"name": "lifetime_value",      "label": "Lifetime Value",       "type": "number",  "field_type": "number"},
    {"name": "last_service_date",   "label": "Last Service Date",    "type": "date",    "field_type": "date"},
]

_HS_DEAL_PROPS = [
    {"name": "proposal_id",            "label": "Proposal ID",               "type": "string", "field_type": "text"},
    {"name": "service_start_date",     "label": "Service Start Date",        "type": "date",   "field_type": "date"},
    {"name": "monthly_contract_value", "label": "Monthly Contract Value",    "type": "number", "field_type": "number"},
]

_HS_LIFECYCLE_STAGES = [
    "subscriber", "lead", "marketingqualifiedlead",
    "salesqualifiedlead", "opportunity", "customer", "evangelist",
]


def _hs_can_write_schemas(client) -> bool:
    """Quick probe: try reading deal properties (always permitted) then test 403 on write."""
    from hubspot.crm.properties import PropertyCreate
    try:
        # Attempt a harmless write that will 403 immediately if scope is missing
        client.crm.properties.core_api.create("contacts", PropertyCreate(
            name="_ss_scope_probe_delete_me",
            label="_probe",
            type="string",
            field_type="text",
            group_name="contactinformation",
        ))
        # If it succeeded, clean up and return True
        try:
            client.crm.properties.core_api.archive("contacts", "_ss_scope_probe_delete_me")
        except Exception:
            pass
        return True
    except Exception as e:
        return "403" not in str(e)  # 403 = missing scope; anything else re-raise


def _hs_ensure_property(client, object_type: str, prop_def: dict, can_write: bool) -> str:
    """Create a HubSpot property if it doesn't exist. Returns property name."""
    from hubspot.crm.properties import PropertyCreate, Option

    name = prop_def["name"]
    try:
        client.crm.properties.core_api.get_by_name(object_type, name)
        _mark("exists", "hubspot", f"  {object_type} property '{name}'")
        return name
    except Exception as e:
        if "404" not in str(e) and "not found" not in str(e).lower():
            raise

    if not can_write:
        _mark("skipped", "hubspot",
              f"  {object_type} property '{name}' (token lacks crm.schemas.*.write scope — create in HubSpot UI)")
        return name

    options = []
    for label, value in prop_def.get("options", []):
        options.append(Option(label=label, value=value, hidden=False, display_order=-1))

    create = PropertyCreate(
        name=name,
        label=prop_def["label"],
        type=prop_def["type"],
        field_type=prop_def["field_type"],
        group_name="contactinformation" if object_type == "contacts" else "dealinformation",
        options=options or None,
    )
    client.crm.properties.core_api.create(object_type, create)
    _mark("created", "hubspot", f"  {object_type} property '{name}'")
    return name


def configure_hubspot() -> dict:
    print("\n── HubSpot ─────────────────────────────────────────────")
    client = get_client("hubspot")
    result = {"contact_properties": [], "deal_properties": [], "lifecycle_stages": []}

    can_write = _hs_can_write_schemas(client)
    if not can_write:
        _mark("noted", "hubspot",
              "token scope: read-only — properties confirmed/listed; grant crm.schemas.*.write to create missing ones")

    for prop_def in _HS_CONTACT_PROPS:
        name = _hs_ensure_property(client, "contacts", prop_def, can_write)
        result["contact_properties"].append(name)

    for prop_def in _HS_DEAL_PROPS:
        name = _hs_ensure_property(client, "deals", prop_def, can_write)
        result["deal_properties"].append(name)

    for stage in _HS_LIFECYCLE_STAGES:
        result["lifecycle_stages"].append(stage)
    _mark("exists", "hubspot", f"  lifecycle stages (built-in): {len(_HS_LIFECYCLE_STAGES)}")

    return result


# ================================================================== #
# ASANA
# ================================================================== #

_ASANA_SECTIONS = {
    "Sales Pipeline Tasks":  ["New Leads", "Contacted", "Site Visit", "Proposal Sent", "Follow-Up", "Closed"],
    "Marketing Calendar":    ["Planning", "In Progress", "Scheduled", "Completed"],
    "Admin & Operations":    ["To Do", "In Progress", "Waiting", "Done"],
    "Client Success":        ["Onboarding", "Active", "At Risk", "Churned"],
}

_ASANA_TEAM_MEMBERS = [
    {"name": "Maria Gonzalez",   "email": "maria@sparkleshine.com"},
    {"name": "Patricia Nguyen",  "email": "patricia@sparkleshine.com"},
    {"name": "Kevin Okafor",     "email": "kevin@sparkleshine.com"},
]


def configure_asana() -> dict:
    print("\n── Asana ───────────────────────────────────────────────")
    api_client = get_client("asana")
    workspace_gid = get_credential("ASANA_WORKSPACE_GID")
    result = {
        "workspace_gid": workspace_gid,
        "team_gid": None,
        "projects": {},
        "sections": {},
        "users": {},
    }

    # ── Team ────────────────────────────────────────────────────────
    existing_team_gid = os.getenv("ASANA_TEAM_GID")
    teams_api = asana_api(api_client, "TeamsApi")
    if existing_team_gid:
        try:
            teams_api.get_team(existing_team_gid, opts={})
            result["team_gid"] = existing_team_gid
            _mark("exists", "asana", f"team (gid={existing_team_gid})")
        except Exception:
            existing_team_gid = None

    if not existing_team_gid:
        teams = list(teams_api.get_teams_for_workspace(workspace_gid, opts={}))
        match = next((t for t in teams if t.get("name") == "Sparkle & Shine"), None)
        if match:
            result["team_gid"] = match["gid"]
            _mark("exists", "asana", "team 'Sparkle & Shine'")
        else:
            team = teams_api.create_team_for_workspace(
                workspace_gid,
                {"body": {"data": {"name": "Sparkle & Shine", "organization": {"gid": workspace_gid}}}},
            )
            result["team_gid"] = team["gid"]
            _mark("created", "asana", "team 'Sparkle & Shine'")

    team_gid = result["team_gid"]

    # ── Projects ────────────────────────────────────────────────────
    _env_project_gids = {
        "Sales Pipeline Tasks":  os.getenv("ASANA_PROJECT_SALES_PIPELINE_GID"),
        "Marketing Calendar":    os.getenv("ASANA_PROJECT_MARKETING_CALENDAR_GID"),
        "Admin & Operations":    os.getenv("ASANA_PROJECT_ADMIN_OPERATIONS_GID"),
        "Client Success":        os.getenv("ASANA_PROJECT_CLIENT_SUCCESS_GID"),
    }

    projects_api = asana_api(api_client, "ProjectsApi")
    sections_api = asana_api(api_client, "SectionsApi")

    # Build lookup of existing projects by name
    existing_projects = {}
    try:
        for proj in projects_api.get_projects_for_team(team_gid, opts={"limit": 100}):
            existing_projects[proj["name"]] = proj["gid"]
    except Exception:
        pass

    for project_name, section_names in _ASANA_SECTIONS.items():
        # Prefer env GID, then existing by name, then create
        proj_gid = _env_project_gids.get(project_name)
        if proj_gid:
            _mark("exists", "asana", f"  project '{project_name}' (env gid)")
        elif project_name in existing_projects:
            proj_gid = existing_projects[project_name]
            _mark("exists", "asana", f"  project '{project_name}'")
        else:
            proj = projects_api.create_project({
                "body": {"data": {
                    "name": project_name,
                    "team": {"gid": team_gid},
                    "workspace": {"gid": workspace_gid},
                }}
            })
            proj_gid = proj["gid"]
            _mark("created", "asana", f"  project '{project_name}'")

        result["projects"][project_name] = proj_gid
        result["sections"][project_name] = {}

        # Sections
        existing_sections = {}
        try:
            for sec in sections_api.get_sections_for_project(proj_gid, opts={}):
                existing_sections[sec["name"]] = sec["gid"]
        except Exception:
            pass

        for sec_name in section_names:
            if sec_name in existing_sections:
                sec_gid = existing_sections[sec_name]
                _mark("exists", "asana", f"    section '{sec_name}'")
            else:
                sec = sections_api.create_section_for_project(
                    proj_gid,
                    {"body": {"data": {"name": sec_name}}},
                )
                sec_gid = sec["gid"]
                _mark("created", "asana", f"    section '{sec_name}'")
            result["sections"][project_name][sec_name] = sec_gid

    # ── Users ────────────────────────────────────────────────────────
    users_api = asana_api(api_client, "UsersApi")
    try:
        existing_users = {
            u.get("email", ""): u
            for u in users_api.get_users_for_workspace(workspace_gid, opts={"opt_fields": "gid,name,email"})
        }
    except Exception:
        existing_users = {}

    for member in _ASANA_TEAM_MEMBERS:
        email = member["email"]
        if email in existing_users:
            result["users"][member["name"]] = existing_users[email]["gid"]
            _mark("exists", "asana", f"  user '{member['name']}'")
        else:
            _mark("skipped", "asana", f"  user '{member['name']}' (invite not attempted — external email)")
            result["users"][member["name"]] = None

    return result


def asana_api(client, api_class: str):
    """Convenience wrapper to get an Asana API instance by class name."""
    import asana
    return getattr(asana, api_class)(client)


# ================================================================== #
# MAILCHIMP
# ================================================================== #

_MC_MERGE_FIELDS = [
    {"tag": "PHONE",        "name": "Phone Number",   "type": "phone"},
    {"tag": "NEIGHBORHD",   "name": "Neighborhood",   "type": "text"},
    {"tag": "CLIENTTYPE",   "name": "Client Type",    "type": "text"},
    {"tag": "SVCTYPE",      "name": "Service Type",   "type": "text"},
    {"tag": "LEADSOURCE",   "name": "Lead Source",    "type": "text"},
]

_MC_SEGMENTS = [
    "Active Residential Clients",
    "Active Commercial Clients",
    "Churned Clients",
    "High-Value Clients",
]

_MC_TAGS = [
    "residential-client", "commercial-client", "active", "churned",
    "recurring-weekly", "recurring-biweekly", "recurring-monthly",
    "vip", "referral",
    "campaign-spring-2025", "campaign-summer-2025",
    "campaign-fall-2025", "campaign-winter-2026",
]


def configure_mailchimp() -> dict:
    print("\n── Mailchimp ───────────────────────────────────────────")
    client = get_client("mailchimp")
    result = {"audience_id": None, "merge_fields": {}, "segments": {}, "tags": _MC_TAGS}

    # ── Audience ────────────────────────────────────────────────────
    env_list_id = os.getenv("MAILCHIMP_LIST_ID")
    audience_name = "Sparkle & Shine Contacts"

    if env_list_id:
        try:
            info = client.lists.get_list(env_list_id)
            result["audience_id"] = env_list_id
            _mark("exists", "mailchimp", f"audience '{info['name']}' (id={env_list_id})")
        except Exception:
            env_list_id = None

    if not env_list_id:
        lists = client.lists.get_all_lists(fields=["lists.id", "lists.name"]).get("lists", [])
        match = next((l for l in lists if l["name"] == audience_name), None)
        if match:
            result["audience_id"] = match["id"]
            _mark("exists", "mailchimp", f"audience '{audience_name}'")
        else:
            new_list = client.lists.create_list({
                "name": audience_name,
                "contact": {
                    "company": "Sparkle & Shine Cleaning Co.",
                    "address1": "4821 Burnet Road Suite 12",
                    "city": "Austin", "state": "TX", "zip": "78756", "country": "US",
                },
                "permission_reminder": "You signed up for updates from Sparkle & Shine.",
                "campaign_defaults": {
                    "from_name": "Sparkle & Shine",
                    "from_email": "maria@sparkleshine.com",
                    "subject": "", "language": "en",
                },
                "email_type_option": False,
            })
            result["audience_id"] = new_list["id"]
            _mark("created", "mailchimp", f"audience '{audience_name}'")

    list_id = result["audience_id"]

    # ── Merge Fields ────────────────────────────────────────────────
    existing_mf = {
        mf["tag"]: mf
        for mf in client.lists.get_list_merge_fields(list_id, count=100).get("merge_fields", [])
    }

    for mf in _MC_MERGE_FIELDS:
        tag = mf["tag"]
        if tag in existing_mf:
            result["merge_fields"][tag] = existing_mf[tag]["merge_id"]
            _mark("exists", "mailchimp", f"  merge field '{tag}'")
        else:
            # Skip EMAIL (built-in) and FNAME/LNAME; only add custom ones
            try:
                created = client.lists.add_list_merge_field(list_id, {
                    "tag": tag, "name": mf["name"], "type": mf["type"],
                })
                result["merge_fields"][tag] = created["merge_id"]
                _mark("created", "mailchimp", f"  merge field '{tag}'")
            except Exception as e:
                _mark("skipped", "mailchimp", f"  merge field '{tag}' ({e})")

    # ── Segments ────────────────────────────────────────────────────
    existing_segs = {
        s["name"]: s["id"]
        for s in client.lists.list_segments(list_id).get("segments", [])
    }

    for seg_name in _MC_SEGMENTS:
        if seg_name in existing_segs:
            result["segments"][seg_name] = existing_segs[seg_name]
            _mark("exists", "mailchimp", f"  segment '{seg_name}'")
        else:
            try:
                seg = client.lists.create_segment(list_id, {
                    "name": seg_name,
                    "options": {"match": "all", "conditions": []},
                })
                result["segments"][seg_name] = seg["id"]
                _mark("created", "mailchimp", f"  segment '{seg_name}'")
            except Exception as e:
                _mark("skipped", "mailchimp", f"  segment '{seg_name}' ({e})")

    _mark("noted", "mailchimp", f"  {len(_MC_TAGS)} tag names registered (applied to contacts on sync)")
    return result


# ================================================================== #
# QUICKBOOKS ONLINE
# ================================================================== #

_QBO_ITEMS = [
    {"Name": "Standard Residential Clean", "UnitPrice": 150.00},
    {"Name": "Deep Clean",                 "UnitPrice": 275.00},
    {"Name": "Move-In/Move-Out Clean",     "UnitPrice": 325.00},
    {"Name": "Recurring Weekly",           "UnitPrice": 135.00},
    {"Name": "Recurring Biweekly",         "UnitPrice": 150.00},
    {"Name": "Recurring Monthly",          "UnitPrice": 165.00},
    {"Name": "Commercial Nightly Clean",   "UnitPrice": 0.00},
    {"Name": "Late Payment Fee",           "UnitPrice": 25.00},
]

_QBO_ACCOUNTS = [
    {"Name": "Cleaning Supplies",       "AccountType": "Cost of Goods Sold"},
    {"Name": "Equipment Maintenance",   "AccountType": "Expense"},
    {"Name": "Vehicle Expenses",        "AccountType": "Expense"},
    {"Name": "Employee Wages",          "AccountType": "Expense"},
    {"Name": "Marketing & Advertising", "AccountType": "Expense"},
    {"Name": "Office Expenses",         "AccountType": "Expense"},
    {"Name": "Insurance",               "AccountType": "Expense"},
    {"Name": "Subcontractors",          "AccountType": "Cost of Goods Sold"},
]


def _qbo_query(headers: dict, base_url: str, entity: str, where: str) -> list:
    """Run a QBO SQL-like query and return rows."""
    import requests as req
    sql = f"SELECT * FROM {entity} WHERE {where}"
    resp = req.get(
        f"{base_url}/query",
        headers=headers,
        params={"query": sql, "minorversion": "65"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("QueryResponse", {})
    return rows.get(entity, [])


def _qbo_post(headers: dict, base_url: str, entity: str, payload: dict) -> dict:
    import requests as req
    resp = req.post(
        f"{base_url}/{entity.lower()}",
        headers=headers,
        json=payload,
        params={"minorversion": "65"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def configure_quickbooks() -> dict:
    print("\n── QuickBooks Online ───────────────────────────────────")
    import requests as _req
    from auth.quickbooks_auth import get_quickbooks_headers, get_base_url
    headers = get_quickbooks_headers()
    base_url = get_base_url()

    # Pre-flight: verify token is valid before running any writes
    company_id = get_credential("QBO_COMPANY_ID")
    probe = _req.get(
        f"{base_url}/companyinfo/{company_id}",
        headers=headers,
        params={"minorversion": "65"},
        timeout=10,
    )
    if probe.status_code == 401:
        raise RuntimeError(
            "QBO access token is expired. Refresh it at developer.intuit.com "
            "or run auth/quickbooks_auth.py run_initial_auth() with valid CLIENT_ID/SECRET."
        )
    result = {"items": {}, "accounts": {}, "income_account_id": None}

    # ── Income Account (needed for Items) ───────────────────────────
    income_name = "Cleaning Services Income"
    rows = _qbo_query(headers, base_url, "Account", f"Name = '{income_name}'")
    if rows:
        income_id = rows[0]["Id"]
        _mark("exists", "quickbooks", f"income account '{income_name}'")
    else:
        data = _qbo_post(headers, base_url, "Account", {
            "Name": income_name,
            "AccountType": "Income",
        })
        income_id = data["Account"]["Id"]
        _mark("created", "quickbooks", f"income account '{income_name}'")
    result["income_account_id"] = income_id

    # ── Service Items ────────────────────────────────────────────────
    for item_def in _QBO_ITEMS:
        name = item_def["Name"]
        escaped = name.replace("'", "\\'")
        rows = _qbo_query(headers, base_url, "Item", f"Name = '{escaped}'")
        if rows:
            result["items"][name] = rows[0]["Id"]
            _mark("exists", "quickbooks", f"  item '{name}'")
        else:
            payload = {
                "Name": name,
                "Type": "Service",
                "UnitPrice": item_def["UnitPrice"],
                "IncomeAccountRef": {"value": income_id},
                "Taxable": False,
            }
            data = _qbo_post(headers, base_url, "Item", payload)
            result["items"][name] = data["Item"]["Id"]
            _mark("created", "quickbooks", f"  item '{name}'")
        time.sleep(0.1)  # light throttle

    # ── Expense Accounts ─────────────────────────────────────────────
    for acct_def in _QBO_ACCOUNTS:
        name = acct_def["Name"]
        escaped = name.replace("'", "\\'")
        rows = _qbo_query(headers, base_url, "Account", f"Name = '{escaped}'")
        if rows:
            result["accounts"][name] = rows[0]["Id"]
            _mark("exists", "quickbooks", f"  account '{name}'")
        else:
            data = _qbo_post(headers, base_url, "Account", {
                "Name": name,
                "AccountType": acct_def["AccountType"],
            })
            result["accounts"][name] = data["Account"]["Id"]
            _mark("created", "quickbooks", f"  account '{name}'")
        time.sleep(0.1)

    return result


# ================================================================== #
# SLACK
# ================================================================== #

_SLACK_CHANNELS = {
    "daily-briefing":       "AI-generated morning briefing — delivered by 6 AM",
    "operations":           "Job completions, scheduling alerts, crew updates",
    "sales":                "New leads, deal updates, commercial proposals",
    "new-clients":          "New client onboarding notifications",
    "reviews-and-feedback": "Customer reviews and satisfaction alerts",
}

# Map channel names to env-var IDs that may already exist
_SLACK_ENV_IDS = {
    "daily-briefing":  os.getenv("SLACK_CHANNEL_DAILY_BRIEFING"),
    "operations":      os.getenv("SLACK_CHANNEL_OPERATIONS"),
    "sales":           os.getenv("SLACK_CHANNEL_SALES"),
    "new-clients":     os.getenv("SLACK_CHANNEL_NEW_CLIENTS"),
}


def configure_slack() -> dict:
    print("\n── Slack ───────────────────────────────────────────────")
    client = get_client("slack")
    result = {"channels": {}}

    # Build name → id map of existing channels
    existing: dict = {}
    cursor = None
    while True:
        kwargs = {"limit": 200, "types": "public_channel", "exclude_archived": True}
        if cursor:
            kwargs["cursor"] = cursor
        resp = client.conversations_list(**kwargs)
        for ch in resp.get("channels", []):
            existing[ch["name"]] = ch["id"]
        cursor = resp.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break

    for ch_name, topic in _SLACK_CHANNELS.items():
        # Check env hint first, then live lookup, then create
        ch_id = _SLACK_ENV_IDS.get(ch_name)
        if ch_id and ch_id in {v for v in existing.values()}:
            _mark("exists", "slack", f"  #{ch_name} (env id={ch_id})")
        elif ch_name in existing:
            ch_id = existing[ch_name]
            _mark("exists", "slack", f"  #{ch_name}")
        else:
            try:
                resp = client.conversations_create(name=ch_name, is_private=False)
                ch_id = resp["channel"]["id"]
                _mark("created", "slack", f"  #{ch_name}")
            except Exception as e:
                err = str(e)
                if "missing_scope" in err or "channels:manage" in err:
                    _mark("skipped", "slack",
                          f"  #{ch_name} (bot needs channels:manage scope — create in Slack UI)")
                    ch_id = None
                else:
                    raise

        result["channels"][ch_name] = ch_id

        # Set topic
        try:
            client.conversations_setTopic(channel=ch_id, topic=topic)
        except Exception:
            pass  # topic set is best-effort

    return result


# ================================================================== #
# MAIN
# ================================================================== #

def main() -> None:
    print("=" * 60)
    print("  Sparkle & Shine — Tool Configuration")
    print("=" * 60)

    tool_ids = _load_tool_ids()
    errors: dict = {}

    runners = [
        ("pipedrive",   configure_pipedrive),
        ("hubspot",     configure_hubspot),
        ("asana",       configure_asana),
        ("mailchimp",   configure_mailchimp),
        ("quickbooks",  configure_quickbooks),
        ("slack",       configure_slack),
    ]

    for key, fn in runners:
        try:
            tool_ids[key] = fn()
        except Exception as exc:
            errors[key] = str(exc)
            print(f"  ERROR in {key}: {exc}")

    _save_tool_ids(tool_ids)

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    if _CREATED:
        print(f"\n  Created ({len(_CREATED)}):")
        for line in _CREATED:
            print(line)
    if _SKIPPED:
        print(f"\n  Already existed ({len(_SKIPPED)}):")
        for line in _SKIPPED:
            print(line)
    if errors:
        print(f"\n  Errors ({len(errors)}):")
        for tool, msg in errors.items():
            print(f"  [{tool}] {msg}")

    print(f"\n  tool_ids.json written → {TOOL_IDS_PATH}")
    total = len(_CREATED) + len(_SKIPPED)
    print(f"  {len(_CREATED)} created, {len(_SKIPPED)} already existed, {len(errors)} tools errored\n")


if __name__ == "__main__":
    main()
