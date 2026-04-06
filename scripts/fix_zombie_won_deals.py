"""
scripts/fix_zombie_won_deals.py

One-off fix for Pipedrive deals that reached the Closed Won stage (stage_id=12)
but still have status='open'. These zombie deals were caused by an API call
that set the stage but failed to persist the won status.

Fixes deals #150, #154, #157 (identified 2026-04-06).

Usage:
    python scripts/fix_zombie_won_deals.py           # dry run (default)
    python scripts/fix_zombie_won_deals.py --apply    # apply fixes
"""

import argparse
import json
import time
from pathlib import Path

from auth import get_client
from intelligence.logging_config import setup_logging

logger = setup_logging("scripts.fix_zombie_won_deals")

ZOMBIE_DEAL_IDS = [150, 154, 157]


def main(apply: bool = False):
    tool_ids = json.loads(Path("config/tool_ids.json").read_text())
    won_stage_id = tool_ids["pipedrive"]["stages"]["Closed Won"]

    client = get_client("pipedrive")

    for deal_id in ZOMBIE_DEAL_IDS:
        time.sleep(0.15)
        resp = client.get(f"https://api.pipedrive.com/v1/deals/{deal_id}")
        resp.raise_for_status()
        deal = resp.json().get("data")
        if not deal:
            logger.warning("Deal %s not found", deal_id)
            continue

        status = deal.get("status")
        stage_id = deal.get("stage_id")
        title = deal.get("title", "?")

        if status == "won":
            logger.info("Deal #%s (%s) already has status=won, skipping", deal_id, title)
            continue

        if stage_id != won_stage_id:
            logger.info(
                "Deal #%s (%s) is at stage %s, not Closed Won (%s), skipping",
                deal_id, title, stage_id, won_stage_id,
            )
            continue

        logger.info(
            "Deal #%s (%s): stage=%s (Closed Won) but status=%s — %s",
            deal_id, title, stage_id, status,
            "FIXING" if apply else "would fix (dry run)",
        )

        if apply:
            time.sleep(0.15)
            resp = client.put(
                f"https://api.pipedrive.com/v1/deals/{deal_id}",
                json={"status": "won"},
            )
            if resp.status_code in (200, 201):
                logger.info("  -> Fixed: deal #%s status set to 'won'", deal_id)
            else:
                logger.error(
                    "  -> Failed to fix deal #%s: %s %s",
                    deal_id, resp.status_code, resp.text[:200],
                )

    logger.info("Done." if apply else "Done (dry run — use --apply to fix).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix zombie won deals in Pipedrive")
    parser.add_argument("--apply", action="store_true", help="Apply fixes (default is dry run)")
    args = parser.parse_args()
    main(apply=args.apply)
