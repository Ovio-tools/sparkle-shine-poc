COMPANY = {
    "name": "Sparkle & Shine Cleaning Co.",
    "address": "4821 Burnet Road, Suite 12, Austin, TX 78756",
    "phone": "(512) 555-0184",
    "email": "maria@sparkleshine.com",
    "website": "www.sparkleshineaustin.com",
    "owner_name": "Maria Gonzalez",
    "founded_year": 2019,
    "annual_revenue_target": 2_000_000,
}

SERVICE_TYPES = [
    {
        "id": "std-residential",
        "name": "Standard Residential Clean",
        "duration_minutes": 120,
        "base_price": 150.00,
        "service_category": "residential",
        "is_recurring": True,
    },
    {
        "id": "deep-clean",
        "name": "Deep Clean",
        "duration_minutes": 210,
        "base_price": 275.00,
        "service_category": "residential",
        "is_recurring": False,
    },
    {
        "id": "move-in-out",
        "name": "Move-In/Move-Out Clean",
        "duration_minutes": 240,
        "base_price": 325.00,
        "service_category": "residential",
        "is_recurring": False,
    },
    {
        "id": "recurring-weekly",
        "name": "Recurring Weekly",
        "duration_minutes": 120,
        "base_price": 135.00,
        "service_category": "residential",
        "is_recurring": True,
    },
    {
        "id": "recurring-biweekly",
        "name": "Recurring Biweekly",
        "duration_minutes": 120,
        "base_price": 150.00,
        "service_category": "residential",
        "is_recurring": True,
    },
    {
        "id": "recurring-monthly",
        "name": "Recurring Monthly",
        "duration_minutes": 120,
        "base_price": 165.00,
        "service_category": "residential",
        "is_recurring": True,
    },
    {
        "id": "commercial-nightly",
        "name": "Commercial Nightly Clean",
        "duration_minutes": 180,
        "base_price": None,
        "price_per_sqft": 0.08,
        "service_category": "commercial",
        "is_recurring": True,
    },
]

EMPLOYEES = [
    {
        "id": "SS-EMP-001",
        "first_name": "Maria",
        "last_name": "Gonzalez",
        "role": "owner",
        "crew": None,
        "hire_date": "2019-03-01",
        "status": "active",
        "hourly_rate": 0,
        "email": "maria.gonzalez@oviodigital.com",
    },
    {
        "id": "SS-EMP-002",
        "first_name": "Claudia",
        "last_name": "Ramirez",
        "role": "team_lead",
        "crew": "A",
        "hire_date": "2023-01-09",
        "status": "active",
        "hourly_rate": 22,
        "email": "claudia.ramirez@oviodigital.com",
    },
    {
        "id": "SS-EMP-003",
        "first_name": "Darnell",
        "last_name": "Washington",
        "role": "team_lead",
        "crew": "B",
        "hire_date": "2023-02-13",
        "status": "active",
        "hourly_rate": 22,
        "email": "darnell.washington@oviodigital.com",
    },
    {
        "id": "SS-EMP-004",
        "first_name": "Patricia",
        "last_name": "Nguyen",
        "role": "office_manager",
        "crew": None,
        "hire_date": "2023-03-06",
        "status": "active",
        "hourly_rate": 25,
        "email": "patricia.nguyen@oviodigital.com",
    },
    {
        "id": "SS-EMP-005",
        "first_name": "Kevin",
        "last_name": "Okafor",
        "role": "sales_estimator",
        "crew": None,
        "hire_date": "2023-04-17",
        "status": "active",
        "hourly_rate": 25,
        "email": "kevin.okafor@oviodigital.com",
    },
    {
        "id": "SS-EMP-006",
        "first_name": "Sandra",
        "last_name": "Flores",
        "role": "bookkeeper",
        "crew": None,
        "hire_date": "2023-05-22",
        "status": "active",
        "hourly_rate": 25,
        "email": "sandra.flores@oviodigital.com",
    },
    # Crew A cleaners
    {
        "id": "SS-EMP-007",
        "first_name": "Leticia",
        "last_name": "Morales",
        "role": "cleaner",
        "crew": "A",
        "hire_date": "2023-06-05",
        "status": "active",
        "hourly_rate": 18,
        "email": "leticia.morales@oviodigital.com",
    },
    {
        "id": "SS-EMP-008",
        "first_name": "Brianna",
        "last_name": "Carter",
        "role": "cleaner",
        "crew": "A",
        "hire_date": "2023-08-14",
        "status": "active",
        "hourly_rate": 18,
        "email": "brianna.carter@oviodigital.com",
    },
    {
        "id": "SS-EMP-009",
        "first_name": "Jorge",
        "last_name": "Espinoza",
        "role": "cleaner",
        "crew": "A",
        "hire_date": "2023-10-02",
        "status": "active",
        "hourly_rate": 18,
        "email": "jorge.espinoza@oviodigital.com",
    },
    # Crew B cleaners
    {
        "id": "SS-EMP-010",
        "first_name": "Tamara",
        "last_name": "Jenkins",
        "role": "cleaner",
        "crew": "B",
        "hire_date": "2023-11-20",
        "status": "active",
        "hourly_rate": 18,
        "email": "tamara.jenkins@oviodigital.com",
    },
    {
        "id": "SS-EMP-011",
        "first_name": "Marcus",
        "last_name": "Thompson",
        "role": "cleaner",
        "crew": "B",
        "hire_date": "2024-01-08",
        "status": "active",
        "hourly_rate": 18,
        "email": "marcus.thompson@oviodigital.com",
    },
    {
        "id": "SS-EMP-012",
        "first_name": "Yolanda",
        "last_name": "Castillo",
        "role": "cleaner",
        "crew": "B",
        "hire_date": "2024-03-18",
        "status": "active",
        "hourly_rate": 18,
        "email": "yolanda.castillo@oviodigital.com",
    },
    # Crew C cleaners
    {
        "id": "SS-EMP-013",
        "first_name": "DeShawn",
        "last_name": "Brooks",
        "role": "cleaner",
        "crew": "C",
        "hire_date": "2024-05-06",
        "status": "active",
        "hourly_rate": 18,
        "email": "deshawn.brooks@oviodigital.com",
    },
    {
        "id": "SS-EMP-014",
        "first_name": "Rosa",
        "last_name": "Gutierrez",
        "role": "cleaner",
        "crew": "C",
        "hire_date": "2024-07-22",
        "status": "active",
        "hourly_rate": 18,
        "email": "rosa.gutierrez@oviodigital.com",
    },
    {
        "id": "SS-EMP-015",
        "first_name": "Ashley",
        "last_name": "Hernandez",
        "role": "cleaner",
        "crew": "C",
        "hire_date": "2024-09-30",
        "status": "active",
        "hourly_rate": 18,
        "email": "ashley.hernandez@oviodigital.com",
    },
    # Crew D cleaners (2 original + 2 hired Jun-Jul 2025, replacing 2 who quit Aug 2025)
    {
        "id": "SS-EMP-016",
        "first_name": "Travis",
        "last_name": "Coleman",
        "role": "team_lead",
        "crew": "D",
        "hire_date": "2024-11-11",
        "status": "active",
        "hourly_rate": 22,
        "email": "travis.coleman@oviodigital.com",
    },
    {
        "id": "SS-EMP-017",
        "first_name": "Vanessa",
        "last_name": "Reyes",
        "role": "cleaner",
        "crew": "D",
        "hire_date": "2025-06-16",
        "status": "active",
        "hourly_rate": 18,
        "email": "vanessa.reyes@oviodigital.com",
    },
    {
        "id": "SS-EMP-018",
        "first_name": "Isaiah",
        "last_name": "Patterson",
        "role": "cleaner",
        "crew": "D",
        "hire_date": "2025-07-07",
        "status": "active",
        "hourly_rate": 18,
        "email": "isaiah.patterson@oviodigital.com",
    },
]

CREWS = [
    {
        "id": "crew-a",
        "name": "Crew A",
        "zone": "West Austin",
        "lead_id": "SS-EMP-002",
        "member_ids": ["SS-EMP-007", "SS-EMP-008", "SS-EMP-009"],
    },
    {
        "id": "crew-b",
        "name": "Crew B",
        "zone": "East Austin",
        "lead_id": "SS-EMP-003",
        "member_ids": ["SS-EMP-010", "SS-EMP-011", "SS-EMP-012"],
    },
    {
        "id": "crew-c",
        "name": "Crew C",
        "zone": "South Austin",
        "lead_id": None,
        "member_ids": ["SS-EMP-013", "SS-EMP-014", "SS-EMP-015"],
    },
    {
        "id": "crew-d",
        "name": "Crew D",
        "zone": "North Austin / Round Rock",
        "lead_id": "SS-EMP-016",
        "member_ids": ["SS-EMP-017", "SS-EMP-018"],
    },
]

ZONES = {
    "crew-a": ["Westlake", "Tarrytown", "Rollingwood", "West Lake Hills"],
    "crew-b": ["East Austin", "Mueller", "Hyde Park", "Cherrywood"],
    "crew-c": ["South Austin", "Zilker", "Travis Heights", "Barton Hills"],
    "crew-d": ["Round Rock", "Cedar Park", "Pflugerville", "North Austin"],
}


def print_summary():
    print("=" * 60)
    print(f"  {COMPANY['name']}")
    print(f"  Owner : {COMPANY['owner_name']}")
    print(f"  Address: {COMPANY['address']}")
    print(f"  Phone  : {COMPANY['phone']}  |  Email: {COMPANY['email']}")
    print(f"  Founded: {COMPANY['founded_year']}  |  Revenue target: ${COMPANY['annual_revenue_target']:,.0f}")
    print("=" * 60)

    print(f"\nSERVICES ({len(SERVICE_TYPES)} types)")
    for s in SERVICE_TYPES:
        price = f"${s['base_price']:.2f}" if s["base_price"] else f"${s['price_per_sqft']}/sqft"
        recur = "recurring" if s["is_recurring"] else "one-time"
        print(f"  [{s['id']}] {s['name']:<30} {price:<12} {s['duration_minutes']} min  {recur}")

    print(f"\nEMPLOYEES ({len(EMPLOYEES)} total)")
    role_counts = {}
    for e in EMPLOYEES:
        role_counts[e["role"]] = role_counts.get(e["role"], 0) + 1
    for role, count in sorted(role_counts.items()):
        print(f"  {role:<20} {count}")

    print(f"\nCREWS ({len(CREWS)} crews)")
    for c in CREWS:
        neighborhoods = ", ".join(ZONES[c["id"]])
        lead = next((e for e in EMPLOYEES if e["id"] == c["lead_id"]), None)
        lead_name = f"{lead['first_name']} {lead['last_name']}" if lead else "unassigned"
        print(f"  {c['name']} — {c['zone']}")
        print(f"    Lead   : {lead_name}")
        print(f"    Members: {len(c['member_ids'])} cleaners")
        print(f"    Zones  : {neighborhoods}")
    print()
