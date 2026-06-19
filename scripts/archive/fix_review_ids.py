#!/usr/bin/env python3
"""Renumber malformed review IDs to the canonical SS-REV-NNNN format.

This repairs legacy IDs such as ``SS-REV-C4726`` that were written by the
job-completion catch-up path and later caused numeric ID generation to fail.

Usage:
    python scripts/fix_review_ids.py           # dry run
    python scripts/fix_review_ids.py --apply   # apply updates
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_connection


def _numeric_suffix(review_id: str) -> int | None:
    if not review_id.startswith("SS-REV-"):
        return None
    suffix = review_id.split("-")[-1]
    return int(suffix) if suffix.isdigit() else None


def main(apply: bool = False) -> None:
    db = get_connection()
    try:
        rows = db.execute(
            "SELECT id, job_id, client_id, review_date FROM reviews ORDER BY review_date, id"
        ).fetchall()

        max_numeric = 0
        malformed_rows = []
        for row in rows:
            review_id = str(row["id"])
            suffix = _numeric_suffix(review_id)
            if suffix is None:
                if review_id.startswith("SS-REV-"):
                    malformed_rows.append(row)
                continue
            max_numeric = max(max_numeric, suffix)

        if not malformed_rows:
            print("No malformed review IDs found.")
            return

        renames = []
        next_numeric = max_numeric
        for row in malformed_rows:
            next_numeric += 1
            renames.append((row["id"], f"SS-REV-{next_numeric:04d}", row["job_id"]))

        print(f"Found {len(malformed_rows)} malformed review ID(s).")
        for old_id, new_id, job_id in renames:
            print(f"  {old_id} -> {new_id} (job {job_id or 'unknown'})")

        if not apply:
            print("Dry run only. Re-run with --apply to update the database.")
            return

        with db:
            for old_id, new_id, _job_id in renames:
                db.execute(
                    "UPDATE reviews SET id = %s WHERE id = %s",
                    (new_id, old_id),
                )

        print(f"Updated {len(renames)} review ID(s).")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates (default is dry run)",
    )
    args = parser.parse_args()
    main(apply=args.apply)
