"""Shared automation assignee routing for the currently active team."""

MARIA_EMAIL = "maria.gonzalez@oviodigital.com"
TOOLS_EMAIL = "tools@oviodigital.com"

_ROLE_EMAILS = {
    "owner": MARIA_EMAIL,
    "bookkeeper": TOOLS_EMAIL,
    "office_manager": TOOLS_EMAIL,
    "sales_estimator": TOOLS_EMAIL,
    "crew_lead": TOOLS_EMAIL,
}


def get_assignee_email(role: str, default: str = TOOLS_EMAIL) -> str:
    return _ROLE_EMAILS.get(role, default)
