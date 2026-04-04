"""
automations/templates/lead_source_openers.py

Personalized opening lines for sales emails, keyed by HubSpot analytics source.
"""

LEAD_SOURCE_OPENERS = {
    "ORGANIC_SEARCH": "Thanks for finding us online",
    "PAID_SEARCH": "Thanks for reaching out through our ad",
    "REFERRAL": "Thanks for getting in touch -- {referrer_name} mentioned you might be looking for a cleaning service",
    "DIRECT_TRAFFIC": "Thanks for visiting our website and reaching out",
    "EMAIL_MARKETING": "Thanks for reaching out after our recent email",
    "SOCIAL_MEDIA": "Thanks for connecting with us",
    "OFFLINE": "Thanks for getting in touch",
    "OTHER": "Thanks for reaching out",
    None: "Thanks for reaching out",
}


def get_opener(lead_source, referrer_name=None):
    """
    Return a personalized opening line for a sales email.

    Parameters
    ----------
    lead_source   : str or None -- HubSpot analytics source value (case-insensitive)
    referrer_name : str or None -- name of the referrer, used only for REFERRAL source

    Returns
    -------
    str
    """
    normalized = lead_source.upper() if lead_source else None
    template = LEAD_SOURCE_OPENERS.get(normalized, LEAD_SOURCE_OPENERS[None])

    if normalized == "REFERRAL":
        if referrer_name:
            return template.format(referrer_name=referrer_name)
        return "Thanks for getting in touch -- we heard you were referred to us"

    return template
