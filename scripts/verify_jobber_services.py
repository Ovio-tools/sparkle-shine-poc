"""
scripts/verify_jobber_services.py

Verify that Jobber has service items matching config/business.py SERVICE_TYPES.
Reports which items exist and which are missing. Does not create items
(Jobber's sandbox may not support productOrServiceCreate).

Usage:
    python scripts/verify_jobber_services.py
"""
from __future__ import annotations

import json
import logging

from auth import get_client
from config.business import SERVICE_TYPES
from seeding.utils.throttler import JOBBER as throttler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_PRODUCTS_QUERY = """
query {
  productsAndServices(first: 50) {
    nodes {
      id
      name
      defaultUnitCost
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def _gql(session, query: str, variables: dict | None = None) -> dict:
    throttler.wait()
    resp = session.post(
        "https://api.getjobber.com/api/graphql",
        json={"query": query, "variables": variables or {}},
        headers={"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    session = get_client("jobber")

    logger.info("Querying Jobber for existing products/services...")
    data = _gql(session, _PRODUCTS_QUERY)

    nodes = (
        data.get("data", {})
        .get("productsAndServices", {})
        .get("nodes", [])
    )

    logger.info("Found %d products/services in Jobber:", len(nodes))
    jobber_items = {}
    for node in nodes:
        logger.info("  [%s] %s — $%s", node["id"], node["name"],
                     node.get("defaultUnitCost", "N/A"))
        jobber_items[node["name"].lower().strip()] = node

    logger.info("")
    logger.info("Comparing against config/business.py SERVICE_TYPES (%d types):", len(SERVICE_TYPES))

    matched = []
    missing = []
    for svc in SERVICE_TYPES:
        name_lower = svc["name"].lower().strip()
        if name_lower in jobber_items:
            matched.append((svc["name"], jobber_items[name_lower]["id"]))
            logger.info("  MATCH: %s → %s", svc["name"], jobber_items[name_lower]["id"])
        else:
            missing.append(svc["name"])
            logger.warning("  MISSING: %s", svc["name"])

    logger.info("")
    logger.info("Summary: %d matched, %d missing", len(matched), len(missing))

    if matched:
        logger.info("")
        logger.info("Matched service IDs (for config/tool_ids.json):")
        service_ids = {}
        for name, jobber_id in matched:
            svc = next(s for s in SERVICE_TYPES if s["name"] == name)
            service_ids[svc["id"]] = jobber_id
        logger.info(json.dumps(service_ids, indent=2))

    if missing:
        logger.info("")
        logger.info("Missing services need to be created in Jobber UI or via API:")
        for name in missing:
            svc = next(s for s in SERVICE_TYPES if s["name"] == name)
            price = f"${svc['base_price']:.2f}" if svc["base_price"] else f"${svc.get('price_per_sqft', 0)}/sqft"
            logger.info("  %s — %s, %d min", name, price, svc["duration_minutes"])


if __name__ == "__main__":
    main()
