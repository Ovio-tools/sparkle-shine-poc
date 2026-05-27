#!/usr/bin/env python3
"""Fix invoices_status_check constraint to include 'written_off'.

The invoices table CHECK constraint only allowed ('draft','sent','paid','overdue'),
but the payment generator sets status to 'written_off' when QBO rejects an invoice.
This caused constraint violations for invoices like SS-INV-3916.

Usage:
    python scripts/fix_invoices_status_constraint.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import get_connection


def main():
    parser = argparse.ArgumentParser(description="Fix invoices status CHECK constraint")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without applying changes")
    args = parser.parse_args()

    db = get_connection()

    # Check if the constraint exists and what it currently allows
    row = db.execute(
        """
        SELECT pg_get_constraintdef(c.oid) AS constraint_def
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        WHERE t.relname = 'invoices'
          AND c.conname = 'invoices_status_check'
        """,
    ).fetchone()

    if row is None:
        print("No invoices_status_check constraint found. Nothing to do.")
        db.close()
        return

    current_def = row["constraint_def"]
    print(f"Current constraint: {current_def}")

    if "written_off" in current_def:
        print("Constraint already includes 'written_off'. Nothing to do.")
        db.close()
        return

    if args.dry_run:
        print("[DRY RUN] Would drop and recreate invoices_status_check to include 'written_off'")
        db.close()
        return

    print("Dropping old constraint and adding updated one...")
    db.execute("ALTER TABLE invoices DROP CONSTRAINT invoices_status_check")
    db.execute(
        "ALTER TABLE invoices ADD CONSTRAINT invoices_status_check "
        "CHECK(status IN ('draft','sent','paid','overdue','written_off'))"
    )
    db.commit()
    print("Done. invoices_status_check now allows: draft, sent, paid, overdue, written_off")

    db.close()


if __name__ == "__main__":
    main()
