"""
automations/templates/signatures.py

Email signature blocks for sales outreach.

Residential emails use Maria's first name for a warm, personal tone.
Commercial and hybrid emails use her full name and title for a professional close.
"""

RESIDENTIAL_SIGNATURE = """\
Maria
Sparkle & Shine Cleaning Co.
(512) 555-0184"""

COMMERCIAL_SIGNATURE = """\
Maria Gonzalez
Owner, Sparkle & Shine Cleaning Co.
(512) 555-0184
info@sparkleshineaustin.com"""


def get_signature(template_set):
    """
    Return the signature block for a given template set.

    Parameters
    ----------
    template_set : str -- "residential", "commercial", or "hybrid"

    Returns
    -------
    str
    """
    if template_set == "residential":
        return RESIDENTIAL_SIGNATURE
    return COMMERCIAL_SIGNATURE
