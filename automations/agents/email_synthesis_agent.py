"""
automations/agents/email_synthesis_agent.py

Agent 3 -- Email Synthesis

Takes contact info from HubSpot and generates a personalised inbound reply
email draft. No external research or job-matching inputs are required.

Public API
----------
    synthesize_email(contact) -> dict | None
"""

from __future__ import annotations

import json
import logging
import re
import time

import anthropic

from automations.templates.signatures import get_signature

__all__ = ["synthesize_email"]

logger = logging.getLogger("automation_07")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODEL = "claude-sonnet-4-6"

_SYSTEM_PROMPT = """\
You are writing a short reply email from Maria Gonzalez, owner of Sparkle & \
Shine Cleaning Co. in Austin, TX, to a lead who just submitted an inquiry \
form on the company website. Write the way a confident small business owner \
would if dashing off a quick reply from her phone between jobs. Not \
corporate. Not salesy. Just a real person responding fast.

You will receive the lead's contact info (name, email, company, contact \
type, area).

TONE RULES:
- No filler phrases. Never say "I'd love to learn more about your needs" \
or "thanks for reaching out to us." Replying fast is the proof of interest.
- No brochure language. Never say things like "we deliver consistent, \
reliable service" or "professional cleaning solutions."
- No scheduling links. Ask a direct question instead.
- Use "we" (not "I") when referring to the company.
- Vary sentence length. Mix short punchy sentences with longer ones. \
Avoid a uniform rhythm that sounds machine-generated.
- Never use em dashes (—). Use a comma, period, or reword instead.

SUBJECT LINE:
Use a natural variation of "Got your inquiry" — for example: \
"Got your message", "Your inquiry", "Re: your inquiry". \
Keep it lowercase-feeling. Do not get creative or polished.

OPENER:
Use a natural variation of "Just saw your form come through" as the first \
sentence after the greeting — for example: "Just got your inquiry", \
"Your form just came in", "Just saw your message come through". \
Do not start with a thank-you.

TEMPLATE SET GUIDANCE:

Residential:
- Use the lead's first name
- Warm, conversational tone
- References to "your home" or "homes like yours"
- Sign off with just "Maria" and title below
- Structure (3 parts, in order):
  1. Opener sentence
  2. Social proof: mention Sparkle & Shine already cleans several homes in \
the lead's area. If a specific sub-city label is provided (e.g. "Cedar Park \
area", "south Austin area"), use it: "We clean several homes in the Cedar \
Park area already, so we know the neighborhood well." If the area is only \
a city name like "Austin area", use city-level phrasing instead: "We clean \
homes all across Austin already." Never say vague filler like "in your area." \
Never name other clients. If area is "Not provided", skip this sentence \
entirely — do not substitute filler.
  3. CTA: ask for bedrooms/bathrooms count and whether they want a one-time \
clean or something recurring. Offer a quick call as an alternative.
  Do NOT propose a site visit or walkthrough for residential.

Commercial:
- Use honorific + last name if a title is known (e.g., "Dr. Rivera"), \
otherwise "Mr./Ms. {Last Name}"
- Professional tone, still personable
- References to "your facility," "your practice," "your office"
- Sign off with full name, title, email, and phone
- Structure (3 parts, in order):
  1. Opener sentence
  2. Social proof: mention Sparkle & Shine already cleans commercial spaces \
in the lead's area. If a specific sub-city label is provided (e.g. "northwest \
Austin area", "Cedar Park area"), use it: "We clean a few commercial spaces \
in the northwest Austin area already, so I know the neighborhood well." If \
the area is only a city name like "Austin area", use city-level phrasing: \
"We clean commercial spaces across Austin already." Never say vague filler \
like "in your area." Never name other clients. If area is "Not provided", \
skip this sentence entirely — do not substitute filler.
  3. CTA: propose a quick walkthrough of their space. Frame it as easy and \
fast ("takes about 15 minutes, quote same day"). End with a direct \
question that invites a reply.

Hybrid (use when contact type is unknown):
- Use the lead's first name (safer than guessing an honorific)
- Professional but approachable tone
- Use neutral language: "your space" or "your property"
- Do not assume residential or commercial; let the lead self-identify
- Sign off with full name, title, email, and phone
- Structure (3 parts, in order):
  1. Opener sentence
  2. Ask what type of space they need cleaned
  3. CTA: offer to get a quote quickly once they share the basics. Invite \
a reply or quick call.

RULES:
- Keep body copy under 75 words (excluding signature)
- Never mention that research was conducted on the lead
- Never reference AI, automation, or agents
- Never repeat the lead's first name more than once in the body

EDGE CASES:
- Missing first name: use "Hi there," as the greeting
- Area is a specific sub-city label (e.g. "Cedar Park area", "south Austin \
area"): use it directly in the social proof sentence.
- Area is a city name only (e.g. "Austin area"): use city-level phrasing \
like "We clean homes all across Austin already." Do not fabricate a \
neighborhood.
- Area is "Not provided": skip the social proof sentence entirely. Do not \
replace it with any filler.
- Missing contact type: use the Hybrid template

EXAMPLES (what good output looks like):

Commercial:
Hi David,
Just saw your form come through. We clean a few commercial spaces in the \
northwest Austin area already, so I know the neighborhood well.
Easiest next step is usually a quick walkthrough of your space. Takes about \
15 minutes, and I can have a quote to you the same day. Want to set \
something up this week?

Residential:
Hi David,
Just saw your form come through. We clean several homes in the Cedar Park \
area already, so we know the neighborhood well.
Happy to put together a quote if you can share a few basics: how many \
bedrooms/bathrooms, and whether you're looking for a one-time clean or \
something recurring. Or if it's easier, I'm happy to hop on a quick call.

Hybrid:
Hi David,
Just saw your form come through. Could you share a bit about the space you \
need cleaned? Whether it's an office, a home, or something else, I can \
usually get a quote together pretty fast once I know the basics.

OUTPUT FORMAT:
Respond with ONLY a JSON object, no preamble, no markdown backticks:
{
  "subject": "string",
  "body": "string",
  "template_set_used": "residential" | "commercial" | "hybrid",
  "variant_used": "inbound",
  "word_count": number
}\
"""

_REQUIRED_KEYS = {"subject", "body", "template_set_used", "variant_used", "word_count"}

# Zip code → sub-city area label for the greater Austin metro.
# For Austin proper, zips map to directional/neighborhood labels.
# For suburbs, zips map to the suburb name.
# _derive_area() appends " area" to produce the final label.
_ZIP_TO_AREA: dict[str, str] = {
    # Downtown / Central Austin
    "78701": "downtown Austin",
    "78702": "east Austin",
    "78703": "central Austin",
    "78704": "south Austin",
    "78705": "central Austin",
    "78756": "central Austin",
    # North Austin
    "78751": "north Austin",
    "78752": "north Austin",
    "78753": "north Austin",
    "78757": "north Austin",
    "78758": "north Austin",
    "78727": "north Austin",
    "78728": "north Austin",
    # Northwest Austin
    "78726": "northwest Austin",
    "78729": "northwest Austin",
    "78730": "northwest Austin",
    "78731": "northwest Austin",
    "78750": "northwest Austin",
    "78759": "northwest Austin",
    # Northeast Austin
    "78724": "northeast Austin",
    "78754": "northeast Austin",
    # East Austin
    "78721": "east Austin",
    "78722": "east Austin",
    "78723": "east Austin",
    "78725": "east Austin",
    # South Austin
    "78739": "south Austin",
    "78745": "south Austin",
    "78748": "south Austin",
    # Southeast Austin
    "78741": "southeast Austin",
    "78742": "southeast Austin",
    "78744": "southeast Austin",
    # Southwest Austin
    "78735": "southwest Austin",
    "78736": "southwest Austin",
    "78737": "southwest Austin",
    "78749": "southwest Austin",
    # West Austin / Hill Country edge
    "78733": "west Austin",
    "78738": "Bee Cave",
    "78746": "Westlake",
    "78732": "Lake Travis",
    "78734": "Lakeway",
    # Suburbs — north/northwest corridor
    "78613": "Cedar Park",
    "78717": "Cedar Park",
    "78641": "Leander",
    "78646": "Leander",
    "78642": "Liberty Hill",
    "78664": "Round Rock",
    "78665": "Round Rock",
    "78681": "Round Rock",
    "78682": "Round Rock",
    "78660": "Pflugerville",
    "78634": "Hutto",
    "78626": "Georgetown",
    "78627": "Georgetown",
    "78628": "Georgetown",
    # Suburbs — south corridor
    "78610": "Buda",
    "78640": "Kyle",
    "78620": "Dripping Springs",
    "78669": "Spicewood",
    "78676": "Wimberley",
    # Suburbs — east
    "78653": "Manor",
    "78617": "Del Valle",
    # Suburb — south/southwest
    "78652": "Manchaca",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derive_area(contact: dict) -> str:
    """Return a readable area label from HubSpot zip and city fields.

    Zip code takes precedence: Austin zips resolve to sub-city labels
    (e.g. 'northwest Austin area'); suburb zips resolve to the suburb name
    (e.g. 'Cedar Park area'). Falls back to city + ' area' when the zip is
    absent or unknown. Returns an empty string when neither is available.
    """
    zip_code = (contact.get("zip") or "").strip()[:5]
    city = (contact.get("city") or "").strip()

    if zip_code and zip_code in _ZIP_TO_AREA:
        return f"{_ZIP_TO_AREA[zip_code]} area"

    if city:
        return f"{city} area"

    return ""


def _build_user_prompt(contact: dict, template_set: str, signature: str) -> str:
    """Build the user message from HubSpot contact fields."""

    def _val(key: str) -> str:
        v = contact.get(key)
        return str(v).strip() if v and str(v).strip() else "Not provided"

    area = _derive_area(contact) or "Not provided"

    return (
        "Write a reply email to this inbound inquiry:\n\n"
        "LEAD INFO:\n"
        f"- Name: {_val('firstname')} {_val('lastname')}\n"
        f"- Email: {_val('email')}\n"
        f"- Company: {_val('company')}\n"
        f"- Contact type: {_val('contact_type')}\n"
        f"- Area: {area}\n\n"
        "TEMPLATE INSTRUCTIONS:\n"
        f"- Template set: {template_set}\n\n"
        f"MARIA'S SIGNATURE:\n{signature}"
    )


def _extract_json(text: str) -> dict:
    """Parse JSON from the model response.

    Strips markdown code fences if present, then attempts json.loads.
    Falls back to a regex search for the first {...} block.
    Raises ValueError if nothing parseable is found.
    """
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise ValueError(f"No valid JSON found in model response: {text[:300]!r}")


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def synthesize_email(contact: dict) -> dict | None:
    """
    Generate a personalised inbound reply email draft.

    Args:
        contact: dict with HubSpot contact fields (firstname, lastname, email,
                 company, contact_type, city, etc.)

    Returns:
        dict with keys: subject, body, template_set_used, variant_used, word_count
        On failure: None (caller handles the fallback)
    """
    contact_type = (contact.get("contact_type") or "").lower().strip()
    if contact_type == "residential":
        template_set = "residential"
    elif contact_type == "commercial":
        template_set = "commercial"
    else:
        template_set = "hybrid"

    signature = get_signature(template_set)

    user_prompt = _build_user_prompt(
        contact=contact,
        template_set=template_set,
        signature=signature,
    )

    client = anthropic.Anthropic(max_retries=0)
    for attempt in range(2):
        try:
            response = client.messages.create(
                model=_MODEL,
                max_tokens=1024,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
            break
        except anthropic.RateLimitError as exc:
            retry_after = 60
            try:
                retry_after = int(exc.response.headers.get("retry-after", 60))
            except Exception:
                pass
            if attempt == 0:
                logger.warning(
                    "Agent 3 rate limited for contact %r %r — waiting %ds before retry.",
                    contact.get("firstname"), contact.get("lastname"), retry_after,
                )
                time.sleep(retry_after)
            else:
                logger.error(
                    "Agent 3 rate limited (retry exhausted) for contact %r %r: %s",
                    contact.get("firstname"), contact.get("lastname"), exc,
                )
                return None
        except Exception as exc:
            logger.error(
                "Anthropic API call failed for contact %r %r: %s",
                contact.get("firstname"),
                contact.get("lastname"),
                exc,
            )
            return None

    # Extract the text content block from the response
    text = ""
    for block in response.content:
        if hasattr(block, "type") and block.type == "text":
            text = block.text
            break

    if not text:
        logger.error(
            "No text block in Anthropic response for contact %r %r",
            contact.get("firstname"),
            contact.get("lastname"),
        )
        return None

    try:
        result = _extract_json(text)
    except (ValueError, Exception) as exc:
        logger.warning(
            "JSON parse failed for contact %r %r: %s | raw text: %s",
            contact.get("firstname"),
            contact.get("lastname"),
            exc,
            text[:500],
        )
        return None

    # Validate required keys
    missing = _REQUIRED_KEYS - result.keys()
    if missing:
        logger.error(
            "Agent 3 response missing keys %s for contact %r %r",
            missing,
            contact.get("firstname"),
            contact.get("lastname"),
        )
        return None

    return result
