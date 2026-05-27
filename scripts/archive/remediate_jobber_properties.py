"""
scripts/remediate_jobber_properties.py

Batch-register jobber_property mappings for all clients that have a Jobber
client mapping but no explicit jobber_property entry.

Most seeded clients (320 of 329) already have the property ID stored in
the `tool_specific_url` column of their `jobber` mapping row. This script
promotes those to proper `jobber_property` mappings. For clients without
a URL, it queries the Jobber API to discover their property ID.

Usage:
    python scripts/remediate_jobber_properties.py           # live
    python scripts/remediate_jobber_properties.py --dry-run  # log only
"""
from __future__ import annotations

import argparse
import logging
import sys

from database.connection import get_connection
from database.mappings import register_mapping, get_tool_id
from auth import get_client
from seeding.utils.throttler import JOBBER as throttler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_CLIENT_PROPERTIES_QUERY = """
query ClientProperties($id: EncodedId!) {
  client(id: $id) {
    clientProperties(first: 1) {
      nodes { id }
    }
  }
}
"""


def _gql(session, query: str, variables: dict) -> dict:
    throttler.wait()
    resp = session.post(
        "https://api.getjobber.com/api/graphql",
        json={"query": query, "variables": variables},
        headers={"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main(dry_run: bool = False) -> None:
    conn = get_connection()
    try:
        # Find clients with jobber mapping but no jobber_property mapping
        rows = conn.execute("""
            SELECT j.canonical_id, j.tool_specific_id, j.tool_specific_url
            FROM cross_tool_mapping j
            LEFT JOIN cross_tool_mapping jp
                ON jp.canonical_id = j.canonical_id AND jp.tool_name = 'jobber_property'
            WHERE j.tool_name = 'jobber'
              AND j.canonical_id LIKE 'SS-CLIENT%%'
              AND jp.canonical_id IS NULL
        """).fetchall()
        rows = [dict(r) for r in rows]

        logger.info("Found %d clients needing jobber_property mapping", len(rows))

        if not rows:
            logger.info("Nothing to do — all clients have property mappings")
            return

        # Phase 1: fast path — promote tool_specific_url to jobber_property
        fast_count = 0
        need_api = []

        for row in rows:
            url = row.get("tool_specific_url")
            if url:
                if dry_run:
                    logger.info("[DRY RUN] Would register jobber_property for %s: %s",
                                row["canonical_id"], url)
                else:
                    register_mapping(row["canonical_id"], "jobber_property", url)
                fast_count += 1
            else:
                need_api.append(row)

        logger.info("Fast-path: %d property mappings registered from tool_specific_url", fast_count)
        logger.info("Need API lookup: %d clients", len(need_api))

        # Phase 2: slow path — query Jobber API
        if need_api and not dry_run:
            session = get_client("jobber")
            api_count = 0
            api_failed = 0

            for row in need_api:
                jobber_client_id = row["tool_specific_id"]
                try:
                    data = _gql(session, _CLIENT_PROPERTIES_QUERY, {"id": jobber_client_id})
                    nodes = (
                        data.get("data", {})
                        .get("client", {})
                        .get("clientProperties", {})
                        .get("nodes", [])
                    )
                    if nodes:
                        prop_id = nodes[0]["id"]
                        register_mapping(row["canonical_id"], "jobber_property", prop_id)
                        api_count += 1
                        logger.info("API: registered property for %s: %s",
                                    row["canonical_id"], prop_id)
                    else:
                        logger.warning("No properties found for %s (Jobber ID: %s)",
                                       row["canonical_id"], jobber_client_id)
                except Exception as e:
                    api_failed += 1
                    logger.error("API failed for %s: %s", row["canonical_id"], e)

            logger.info("API path: %d registered, %d failed", api_count, api_failed)
        elif need_api and dry_run:
            for row in need_api:
                logger.info("[DRY RUN] Would query Jobber API for %s", row["canonical_id"])

        # Summary
        total_after = conn.execute(
            "SELECT COUNT(*) AS cnt FROM cross_tool_mapping WHERE tool_name = 'jobber_property'"
        ).fetchone()["cnt"]
        logger.info("Total jobber_property mappings now: %d", total_after)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remediate Jobber property mappings")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log actions without making changes")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
