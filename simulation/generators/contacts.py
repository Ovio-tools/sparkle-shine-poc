"""
simulation/generators/contacts.py

Creates new HubSpot contacts from scratch, one per execute_one() call.
Lifecycle stage distribution ensures steady SQL flow to trigger the
HubSpot-to-Pipedrive automation.

Build step (generates simulation/data/names.json and simulation/data/addresses.json):
    cd sparkle-shine-poc && python -m simulation.generators.contacts --build
"""

import json
import os
import random
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from auth import get_client
from database.mappings import generate_id, get_tool_id, register_mapping
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import HUBSPOT as throttler
from simulation.config import DAILY_VOLUMES

logger = setup_logging("simulation.contacts")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).parent.parent / "data"
_NAMES_FILE = _DATA_DIR / "names.json"
_ADDRESSES_FILE = _DATA_DIR / "addresses.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EMAIL_DOMAINS = [
    ("gmail.com",   0.60),
    ("yahoo.com",   0.15),
    ("outlook.com", 0.10),
    ("icloud.com",  0.05),
    # work domains — 10% combined
    ("austintx.gov",       0.02),
    ("utexas.edu",         0.02),
    ("dell.com",           0.02),
    ("apple.com",          0.02),
    ("solarwinds.com",     0.02),
]

_LEAD_SOURCES = [
    ("referral",       0.25),
    ("google_organic", 0.20),
    ("google_ads",     0.20),
    ("yelp",           0.10),
    ("nextdoor",       0.10),
    ("facebook",       0.05),
    ("direct",         0.10),
]

_LIFECYCLE_STAGES = [
    ("subscriber",            0.20),
    ("lead",                  0.25),
    ("marketing_qualified_lead", 0.20),
    ("sales_qualified_lead",  0.35),
]

_COMMERCIAL_SQL_PROBABILITY = 0.50

# Lifecycle stage → leads.status mapping
_STAGE_TO_STATUS = {
    "subscriber":               "new",
    "lead":                     "new",
    "marketing_qualified_lead": "contacted",
    "sales_qualified_lead":     "qualified",
}

_COMPANY_ADJECTIVES = [
    "Austin", "Texas", "Lone Star", "Capital City", "Barton Creek",
    "South Congress", "Sixth Street", "Lamar", "Highland", "Westlake",
]
_COMPANY_NOUNS = [
    "Properties", "Realty", "Medical Group", "Consulting", "Ventures",
    "Holdings", "Services", "Solutions", "Partners", "Group",
]
_COMPANY_TYPES = [
    "LLC", "Inc.", "Co.", "Group", "& Associates",
]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class GeneratorResult:
    """Returned by every execute_one() call."""
    summary: str
    tool: str
    canonical_id: str
    details: dict = field(default_factory=dict)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Generator class
# ---------------------------------------------------------------------------

class ContactGenerator:
    """
    Creates new HubSpot contacts from scratch.

    Registered with the simulation engine as the "contacts" generator.
    Engine calls execute_one() on each tick when this generator
    hasn't hit its daily target yet.
    """

    name = "contacts"

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path
        self.logger = logger
        self._names: dict | None = None
        self._addresses: dict | None = None

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    async def execute_one(self) -> GeneratorResult:
        """Create one new HubSpot contact and register the canonical record."""
        profile = self.generate_contact_profile()
        lifecycle_stage = self.assign_lifecycle_stage(profile)

        db = sqlite3.connect(self.db_path)
        try:
            # ── Duplicate check (L14) ────────────────────────────────────────
            # Check if a lead with this email already has a HubSpot mapping.
            row = db.execute(
                "SELECT id FROM leads WHERE email = ?", (profile["email"],)
            ).fetchone()
            if row is not None:
                existing_lead_id = row[0]
                if get_tool_id(existing_lead_id, "hubspot", self.db_path):
                    self.logger.info(
                        "Skipping %s — already mapped to HubSpot", profile["email"]
                    )
                    return GeneratorResult(
                        summary=f"Skipped duplicate email: {profile['email']}",
                        tool="hubspot",
                        canonical_id=existing_lead_id,
                    )

            # ── Atomic: generate_id → create in HubSpot → link (L9) ─────────
            canonical_id = generate_id("LEAD", self.db_path)

            # Write SQLite record first so the row exists before the API call
            self._insert_lead(db, canonical_id, profile, lifecycle_stage)

            # Create in HubSpot (includes canonical ID as note)
            hubspot_id = self._create_in_hubspot(canonical_id, profile, lifecycle_stage)

            # Register HubSpot mapping
            register_mapping(canonical_id, "hubspot", hubspot_id, db_path=self.db_path)

            # CRITICAL SQL MAPPING RULE: Do NOT register Pipedrive mapping here.
            # The automation runner detects new SQLs by finding HubSpot contacts
            # with NO Pipedrive entry in cross_tool_mapping. Registering one here
            # would cause the runner to skip this contact forever.

            db.commit()

            self.logger.info(
                "Created contact %s (%s %s, %s) → HubSpot %s",
                canonical_id, profile["first_name"], profile["last_name"],
                lifecycle_stage, hubspot_id,
            )
            return GeneratorResult(
                summary=(
                    f"Created contact: {profile['first_name']} {profile['last_name']}"
                    f" ({lifecycle_stage})"
                ),
                tool="hubspot",
                canonical_id=canonical_id,
                details={
                    "hubspot_id": hubspot_id,
                    "email": profile["email"],
                    "lifecycle_stage": lifecycle_stage,
                    "client_type": profile["client_type"],
                    "lead_source": profile["lead_source"],
                },
            )

        except Exception as e:
            db.rollback()
            self.logger.error("ContactGenerator failed: %s", e)
            raise

        finally:
            db.close()

    # -----------------------------------------------------------------------
    # Profile generation
    # -----------------------------------------------------------------------

    def generate_contact_profile(self) -> dict:
        names = self._load_names()
        addresses = self._load_addresses()

        first_name = random.choice(names["first_names"])
        last_name = random.choice(names["last_names"])
        email = self._generate_email(first_name, last_name)
        phone = self._generate_phone()
        client_type = "commercial" if random.random() < 0.15 else "residential"
        lead_source = self._weighted_choice(_LEAD_SOURCES)
        crew_zone, zip_code, street = self._pick_address(addresses)
        house_number = random.randint(100, 9999)
        address = f"{house_number} {street}"

        profile = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "address": address,
            "city": "Austin",
            "state": "TX",
            "zip": zip_code,
            "neighborhood": crew_zone,
            "client_type": client_type,
            "lead_source": lead_source,
            "service_interest": self._pick_service_interest(client_type),
        }

        if client_type == "commercial":
            profile["company_name"] = self._generate_company_name()

        return profile

    def assign_lifecycle_stage(self, profile: dict) -> str:
        if profile["client_type"] == "commercial":
            if random.random() < _COMMERCIAL_SQL_PROBABILITY:
                return "sales_qualified_lead"
            # Fall back to non-SQL stages only — SQL is handled by the 50% check above
            non_sql = [(s, w) for s, w in _LIFECYCLE_STAGES if s != "sales_qualified_lead"]
            return self._weighted_choice(non_sql)
        return self._weighted_choice(_LIFECYCLE_STAGES)

    # -----------------------------------------------------------------------
    # HubSpot API
    # -----------------------------------------------------------------------

    def _create_in_hubspot(
        self, canonical_id: str, profile: dict, lifecycle_stage: str
    ) -> str:
        session = get_client("hubspot")
        throttler.wait()

        properties = {
            "email":               profile["email"],
            "firstname":           profile["first_name"],
            "lastname":            profile["last_name"],
            "phone":               profile["phone"],
            "address":             profile["address"],
            "city":                profile["city"],
            "state":               profile["state"],
            "zip":                 profile["zip"],
            "lifecyclestage":      lifecycle_stage,
            "client_type":         profile["client_type"],
            "lead_source_detail":  profile["lead_source"],
            "service_interest":    profile["service_interest"],
            "hs_lead_status":      "NEW",
            # L20: embed canonical ID for traceability
            "notes": f"SS-ID: {canonical_id}",
        }
        if profile.get("company_name"):
            properties["company"] = profile["company_name"]

        resp = session.post(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            json={"properties": properties},
            timeout=30,
        )
        if resp.status_code == 409:
            # Contact already exists in HubSpot — extract existing ID
            existing_id = resp.json().get("message", "")
            match = re.search(r"Existing ID: (\d+)", existing_id)
            if match:
                return match.group(1)
            raise RuntimeError(f"HubSpot 409 but no ID in response: {resp.text[:300]}")
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"HubSpot API error {resp.status_code}: {resp.text[:300]}"
            )
        return resp.json()["id"]

    # -----------------------------------------------------------------------
    # SQLite write
    # -----------------------------------------------------------------------

    def _insert_lead(
        self,
        db: sqlite3.Connection,
        canonical_id: str,
        profile: dict,
        lifecycle_stage: str,
    ) -> None:
        status = _STAGE_TO_STATUS.get(lifecycle_stage, "new")
        estimated_value = self._estimate_value(profile["client_type"], lifecycle_stage)
        notes = (
            f"lifecycle_stage: {lifecycle_stage} | "
            f"address: {profile['address']}, {profile['city']}, "
            f"{profile['state']} {profile['zip']}"
        )
        db.execute(
            """
            INSERT INTO leads (
                id, first_name, last_name, company_name, email, phone,
                lead_type, source, status, estimated_value, created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical_id,
                profile["first_name"],
                profile["last_name"],
                profile.get("company_name"),
                profile["email"],
                profile["phone"],
                profile["client_type"],
                profile["lead_source"],
                status,
                estimated_value,
                datetime.utcnow().isoformat(),
                notes,
            ),
        )

    # -----------------------------------------------------------------------
    # Data loaders
    # -----------------------------------------------------------------------

    def _load_names(self) -> dict:
        if self._names is None:
            with open(_NAMES_FILE) as f:
                self._names = json.load(f)
        return self._names

    def _load_addresses(self) -> dict:
        if self._addresses is None:
            with open(_ADDRESSES_FILE) as f:
                self._addresses = json.load(f)
        return self._addresses

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _weighted_choice(options: list[tuple]) -> str:
        choices, weights = zip(*options)
        return random.choices(choices, weights=weights, k=1)[0]

    @staticmethod
    def _generate_email(first_name: str, last_name: str) -> str:
        domains = [d for d, _ in _EMAIL_DOMAINS]
        weights = [w for _, w in _EMAIL_DOMAINS]
        domain = random.choices(domains, weights=weights, k=1)[0]
        # Add a random suffix occasionally to reduce duplicates
        suffix = str(random.randint(1, 999)) if random.random() < 0.3 else ""
        return f"{first_name.lower()}.{last_name.lower()}{suffix}@{domain}"

    @staticmethod
    def _generate_phone() -> str:
        area_code = "512" if random.random() < 0.80 else "737"
        exchange = random.randint(200, 999)
        number = random.randint(1000, 9999)
        return f"({area_code}) {exchange}-{number}"

    @staticmethod
    def _pick_address(addresses: dict) -> tuple[str, str, str]:
        """Returns (crew_zone_label, zip_code, street_name)."""
        zone_key = random.choices(
            ["crew_a", "crew_b", "crew_c", "crew_d"],
            weights=[0.30, 0.25, 0.25, 0.20],
            k=1,
        )[0]
        zone_labels = {
            "crew_a": "Westlake/Tarrytown",
            "crew_b": "East Austin/Mueller",
            "crew_c": "South Austin/Zilker",
            "crew_d": "Round Rock/Cedar Park",
        }
        zone_data = addresses[zone_key]
        zip_code = random.choice(zone_data["zips"])
        street = random.choice(zone_data["streets"])
        return zone_labels[zone_key], zip_code, street

    @staticmethod
    def _pick_service_interest(client_type: str) -> str:
        if client_type == "commercial":
            return random.choice(["nightly_clean", "weekend_deep_clean", "one_time_project"])
        return random.choices(
            ["weekly_recurring", "biweekly_recurring", "monthly_recurring",
             "one_time_standard", "one_time_deep_clean", "one_time_move_in_out"],
            weights=[0.25, 0.35, 0.15, 0.10, 0.10, 0.05],
            k=1,
        )[0]

    @staticmethod
    def _generate_company_name() -> str:
        adj = random.choice(_COMPANY_ADJECTIVES)
        noun = random.choice(_COMPANY_NOUNS)
        suffix = random.choice(_COMPANY_TYPES)
        return f"{adj} {noun} {suffix}"

    @staticmethod
    def _estimate_value(client_type: str, lifecycle_stage: str) -> float:
        """Rough monthly value estimate for pipeline tracking."""
        if client_type == "commercial":
            return random.uniform(1500, 6000)
        if lifecycle_stage == "sales_qualified_lead":
            return random.uniform(150, 400)
        return 0.0


# ---------------------------------------------------------------------------
# Build step — generates simulation/data/*.json via Anthropic API
# Run once: python -m simulation.generators.contacts --build
# ---------------------------------------------------------------------------

def build_data_files() -> None:
    """Call Claude API to generate names.json and addresses.json."""
    import anthropic
    import re as _re

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set")

    ai = anthropic.Anthropic(api_key=api_key)
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    # --- Names ---
    names_prompt = """Generate a realistic list of names reflecting Austin, TX demographics.

Return ONLY valid JSON:
{
  "first_names": [...],
  "last_names": [...]
}

- 220 first names: ~90 Anglo, ~65 Hispanic, ~35 Asian, ~30 Black, mix of male/female
- 220 last names: ~80 Anglo, ~70 Hispanic, ~40 Asian, ~30 Black
- No duplicates within each list"""

    print("Generating names via Claude API...")
    r = ai.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": names_prompt}],
    )
    m = _re.search(r"\{[\s\S]*\}", r.content[0].text)
    names_data = json.loads(m.group(0))
    with open(_NAMES_FILE, "w") as f:
        json.dump(names_data, f, indent=2)
    print(f"  Saved {len(names_data['first_names'])} first names, "
          f"{len(names_data['last_names'])} last names → {_NAMES_FILE}")

    # --- Addresses ---
    addr_prompt = """Generate realistic Austin, TX street names for a cleaning company simulation.

Return ONLY valid JSON:
{
  "crew_a": {"zips": ["78746","78703","78731","78733"], "streets": [...]},
  "crew_b": {"zips": ["78702","78722","78723","78741"], "streets": [...]},
  "crew_c": {"zips": ["78704","78745","78748","78749"], "streets": [...]},
  "crew_d": {"zips": ["78681","78613","78665","78664"], "streets": [...]}
}

55 streets per zone:
- crew_a: Westlake/Tarrytown (upscale, lakefront, hills)
- crew_b: East Austin/Mueller (urban, diverse)
- crew_c: South Austin/Zilker (hip, eclectic)
- crew_d: Round Rock/Cedar Park (suburban, newer)
Use realistic Austin street names and types (Dr, Ln, Ave, Blvd, Ct, Way, Pl, Rd, St, Cir, Trail)."""

    print("Generating addresses via Claude API...")
    r2 = ai.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": addr_prompt}],
    )
    m2 = _re.search(r"\{[\s\S]*\}", r2.content[0].text)
    addr_data = json.loads(m2.group(0))
    with open(_ADDRESSES_FILE, "w") as f:
        json.dump(addr_data, f, indent=2)
    for zone, data in addr_data.items():
        print(f"  {zone}: {len(data['streets'])} streets")
    print(f"  Saved → {_ADDRESSES_FILE}")


if __name__ == "__main__":
    import sys
    if "--build" in sys.argv:
        build_data_files()
    else:
        print("Usage: python -m simulation.generators.contacts --build")
        print("  Generates simulation/data/names.json and simulation/data/addresses.json")
