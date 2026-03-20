"""
Deterministic fake-data generator for Austin, TX seeding.

Seeded with random.seed(42) for reproducibility across runs.
No external Faker dependency — pure Python stdlib.

Usage:
    from seeding.utils.faker_austin import fake_person, fake_address

    person = fake_person(seed_offset=0)
    address = fake_address("South Austin")
"""

import random
import re
import unicodedata
from datetime import date, timedelta
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Data pools
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    # Anglo / broadly common
    "Ashley", "Brittany", "Chelsea", "Danielle", "Emily", "Heather", "Jennifer",
    "Jessica", "Katherine", "Lauren", "Megan", "Nicole", "Rachel", "Rebecca",
    "Samantha", "Sarah", "Stephanie", "Taylor", "Amanda", "Amber",
    # Hispanic / Latina (heavy Austin presence)
    "Alejandra", "Alicia", "Ana", "Claudia", "Elena", "Esperanza", "Gloria",
    "Guadalupe", "Isabel", "Leticia", "Lucía", "Maria", "Marisol", "Monica",
    "Patricia", "Rosa", "Sandra", "Sofia", "Valeria", "Vanessa",
    # Other common Austin demographics
    "Aisha", "Brianna", "Destiny", "Diamond", "Jasmine", "Keisha", "Latoya",
    "Maya", "Monique", "Tiffany",
    # Male names (minority in cleaning industry staffing pool)
    "Carlos", "David", "Eduardo", "Jose", "Juan", "Luis", "Manuel", "Miguel",
    "Ricardo", "Roberto", "Chris", "James", "Jason", "Kevin", "Marcus",
    "Michael", "Nathan", "Ryan", "Sean", "Tyler",
]

LAST_NAMES = [
    # Anglo
    "Anderson", "Baker", "Campbell", "Carter", "Clark", "Collins", "Davis",
    "Evans", "Foster", "Garcia", "Hall", "Harris", "Jackson", "Johnson",
    "Jones", "King", "Lee", "Lewis", "Martin", "Martinez",
    # Hispanic / Latino
    "Acosta", "Aguilar", "Castillo", "Chavez", "Cruz", "Flores", "Gonzalez",
    "Gutierrez", "Hernandez", "Lopez", "Morales", "Ortiz", "Ramirez",
    "Rivera", "Rodriguez", "Sanchez", "Torres", "Vargas", "Vega", "Reyes",
    # Other common Austin surnames
    "Brown", "Cooper", "Edwards", "Green", "Hill", "Mitchell", "Moore",
    "Murphy", "Nelson", "Parker", "Patel", "Robinson", "Scott", "Smith",
    "Taylor", "Thomas", "Thompson", "Turner", "Walker", "White",
]

AUSTIN_STREETS = [
    "Lamar Blvd", "Burnet Rd", "South Congress Ave", "Barton Springs Rd",
    "Cesar Chavez St", "Sixth St", "Seventh St", "Eighth St",
    "Oltorf St", "William Cannon Dr", "Ben White Blvd", "Slaughter Ln",
    "Research Blvd", "Rundberg Ln", "Anderson Ln", "Parmer Ln",
    "Braker Ln", "Howard Ln", "McNeil Dr", "N Mopac Expy",
    "S Mopac Expy", "I-35 Frontage Rd", "Airport Blvd", "Mueller Blvd",
    "Manor Rd", "Springdale Rd", "Riverside Dr", "Montopolis Dr",
    "Pleasant Valley Rd", "Webberville Rd", "E Martin Luther King Jr Blvd",
    "W Martin Luther King Jr Blvd", "Guadalupe St", "West Ave",
    "Shoal Creek Blvd", "Great Hills Trail", "Spicewood Springs Rd",
    "Mesa Dr", "Far West Blvd", "Bull Creek Rd",
]

AUSTIN_BUSINESSES = [
    "Barton Creek Medical Group", "South Lamar Dental", "Mueller Tech Suites",
    "Rosedale Family Practice", "Allandale Veterinary Clinic",
    "Crestview Coworking", "Hyde Park Realty Group", "Brentwood Law Offices",
    "North Loop Bistro", "East Cesar Chavez Gallery",
    "Travis County Title Co", "Domain Business Center",
    "Westlake Financial Partners", "Lakeway Sports Medicine",
    "Round Rock Pediatric Dentistry", "Pflugerville Urgent Care",
    "Cedar Park Chiropractic", "Georgetown Insurance Agency",
    "Steiner Ranch Salon & Spa", "Bee Cave Wellness Center",
    "Austin Body Works", "Capitol View Accounting",
    "Downtown Austin Yoga", "Rainey Street Management",
    "Red River Creative Studios", "East Austin Brewing Co",
    "Cherrywood Coffeehouse LLC", "Montopolis Community Clinic",
    "Sunset Valley Pet Hospital", "Oak Hill Learning Center",
]

PHONE_PREFIXES = [512, 737]

# ---------------------------------------------------------------------------
# Neighborhood → ZIP lookup (approximate)
# ---------------------------------------------------------------------------

_NEIGHBORHOOD_ZIPS: Dict[str, str] = {
    "West Austin": "78703",
    "East Austin": "78721",
    "South Austin": "78704",
    "North Austin": "78758",
    "Round Rock": "78664",
    "Cedar Park": "78613",
    "Pflugerville": "78660",
    "Lakeway": "78734",
    "Westlake": "78746",
    "Downtown": "78701",
    "Mueller": "78723",
    "Hyde Park": "78751",
    "Crestview": "78752",
    "Allandale": "78757",
    "Rosedale": "78756",
    "Brentwood": "78757",
    "Cherrywood": "78722",
    "Rainey Street": "78701",
}
_DEFAULT_ZIP = "78750"

# ---------------------------------------------------------------------------
# Internal RNG — isolated so calls with the same seed_offset always produce
# the same result regardless of call order.
# ---------------------------------------------------------------------------

_rng = random.Random(42)


def _seeded_choice(pool: list, offset: int) -> str:
    r = random.Random(42 + offset)
    return r.choice(pool)


def _seeded_int(low: int, high: int, offset: int) -> int:
    r = random.Random(42 + offset)
    return r.randint(low, high)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def fake_email(first: str, last: str, company: Optional[str] = None) -> str:
    """
    Generate a realistic email address.

    Residential clients get a consumer domain (gmail/yahoo/icloud).
    Commercial clients get a domain derived from the company name.
    """
    first_clean = unicodedata.normalize("NFKD", first.lower()).encode("ascii", "ignore").decode().replace(" ", "").replace("'", "")
    last_clean = unicodedata.normalize("NFKD", last.lower()).encode("ascii", "ignore").decode().replace(" ", "").replace("'", "")

    if company:
        # derive a slug from the company name
        slug = re.sub(r"[^a-z0-9]", "", company.lower())[:20]
        domain = f"{slug}.com"
        return f"{first_clean}.{last_clean}@{domain}"
    else:
        offset = sum(ord(c) for c in first_clean + last_clean)
        domain = _seeded_choice(["gmail.com", "yahoo.com", "icloud.com"], offset)
        sep = _seeded_choice([".", "_", ""], offset + 1)
        return f"{first_clean}{sep}{last_clean}@{domain}"


def fake_person(seed_offset: int = 0) -> dict:
    """
    Return a dict with keys: first_name, last_name, email, phone.
    """
    first = _seeded_choice(FIRST_NAMES, seed_offset)
    last = _seeded_choice(LAST_NAMES, seed_offset + 1)
    area_code = _seeded_choice(PHONE_PREFIXES, seed_offset + 2)
    exchange = _seeded_int(200, 999, seed_offset + 3)
    number = _seeded_int(1000, 9999, seed_offset + 4)
    phone = f"({area_code}) {exchange}-{number}"
    email = fake_email(first, last)
    return {
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": phone,
    }


def fake_address(neighborhood: str) -> dict:
    """
    Return a dict with keys: street, city, state, zip.
    ZIP codes roughly match the given neighborhood.
    """
    offset = sum(ord(c) for c in neighborhood)
    street_num = _seeded_int(100, 9999, offset)
    street_name = _seeded_choice(AUSTIN_STREETS, offset + 1)
    zip_code = _NEIGHBORHOOD_ZIPS.get(neighborhood, _DEFAULT_ZIP)

    # Round Rock and Cedar Park are technically separate cities
    if "Round Rock" in neighborhood:
        city = "Round Rock"
    elif "Cedar Park" in neighborhood:
        city = "Cedar Park"
    elif "Pflugerville" in neighborhood:
        city = "Pflugerville"
    elif "Lakeway" in neighborhood or "Westlake" in neighborhood:
        city = "Austin"
    else:
        city = "Austin"

    return {
        "street": f"{street_num} {street_name}",
        "city": city,
        "state": "TX",
        "zip": zip_code,
    }


def fake_business(seed_offset: int = 0) -> dict:
    """
    Return a dict with keys: company_name, contact_name, email, phone, address.
    """
    company_name = _seeded_choice(AUSTIN_BUSINESSES, seed_offset)
    contact = fake_person(seed_offset + 100)
    email = fake_email(contact["first_name"], contact["last_name"], company=company_name)
    address = fake_address(_seeded_choice(list(_NEIGHBORHOOD_ZIPS.keys()), seed_offset + 200))
    return {
        "company_name": company_name,
        "contact_name": f"{contact['first_name']} {contact['last_name']}",
        "email": email,
        "phone": contact["phone"],
        "address": address,
    }


def fake_date_in_range(start: str, end: str) -> str:
    """
    Return a random date string (YYYY-MM-DD) between start and end inclusive.

    Args:
        start: ISO date string, e.g. "2025-04-01"
        end:   ISO date string, e.g. "2026-03-31"
    """
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    delta = (end_date - start_date).days
    if delta < 0:
        raise ValueError(f"start ({start}) must be before end ({end})")
    random_days = _rng.randint(0, delta)
    return (start_date + timedelta(days=random_days)).isoformat()


def fake_time_slot() -> str:
    """
    Return one of the realistic job start times used by Sparkle & Shine.
    """
    return _rng.choice(["08:00", "10:00", "13:00", "15:00"])
