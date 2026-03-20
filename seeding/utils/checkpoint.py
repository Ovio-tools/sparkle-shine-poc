"""
Lightweight file-based checkpoint system for resumable data pushes.

Checkpoints are stored as JSON files in a checkpoints/ directory so that a
seeding run can be interrupted and resumed from the last successful record.

Usage:
    from seeding.utils.checkpoint import CheckpointIterator

    for record in CheckpointIterator("push_jobber_clients", records):
        push_to_jobber(record)
"""

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

CHECKPOINT_DIR = Path(__file__).resolve().parent.parent.parent / "checkpoints"
AUTOSAVE_EVERY = 25


def _checkpoint_path(job_name: str) -> Path:
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    return CHECKPOINT_DIR / f"{job_name}.json"


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def save_checkpoint(
    job_name: str,
    last_completed_id: str,
    metadata: Optional[Dict] = None,
) -> None:
    """
    Write checkpoint state to checkpoints/{job_name}.json.

    Args:
        job_name: Unique identifier for this seeding job.
        last_completed_id: The canonical ID of the last successfully processed record.
        metadata: Optional extra data to persist alongside the checkpoint.
    """
    payload = {
        "last_completed_id": last_completed_id,
        "metadata": metadata or {},
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    path = _checkpoint_path(job_name)
    path.write_text(json.dumps(payload, indent=2))


def load_checkpoint(job_name: str) -> Optional[Dict]:
    """
    Return the saved checkpoint dict for job_name, or None if none exists.
    """
    path = _checkpoint_path(job_name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def clear_checkpoint(job_name: str) -> None:
    """Delete the checkpoint file (call on successful completion)."""
    path = _checkpoint_path(job_name)
    if path.exists():
        path.unlink()


def is_completed(job_name: str, canonical_id: str) -> bool:
    """
    Return True if canonical_id appears at or before the last checkpoint.

    This is used internally by CheckpointIterator to skip already-processed
    records.  It performs a simple string equality check against the stored
    last_completed_id; the iterator handles positional skipping.
    """
    checkpoint = load_checkpoint(job_name)
    if checkpoint is None:
        return False
    return checkpoint.get("last_completed_id") == canonical_id


# ---------------------------------------------------------------------------
# CheckpointIterator
# ---------------------------------------------------------------------------

class CheckpointIterator:
    """
    Wraps a list of records, skipping those already processed according to the
    stored checkpoint, autosaving every 25 records, and saving on interruption.

    Args:
        job_name:  Unique name for this job (used as the checkpoint filename).
        items:     Ordered list of record dicts to iterate.
        id_field:  Key in each record dict that holds the canonical ID.

    Example::

        records = [{"id": "SS-CLIENT-0001", ...}, ...]
        for record in CheckpointIterator("push_jobber_clients", records):
            push_to_jobber(record)
    """

    def __init__(self, job_name: str, items: list[dict], id_field: str = "id"):
        self.job_name = job_name
        self.items = items
        self.id_field = id_field
        self._checkpoint = load_checkpoint(job_name)
        self._last_id_saved: Optional[str] = None

    def __iter__(self):
        checkpoint = self._checkpoint
        resume_id = checkpoint["last_completed_id"] if checkpoint else None

        # If we have a checkpoint, skip records up to and including resume_id.
        skipping = resume_id is not None
        processed_since_save = 0

        try:
            for record in self.items:
                record_id = record.get(self.id_field)

                if skipping:
                    if record_id == resume_id:
                        skipping = False
                    # Skip this record (already done)
                    continue

                yield record

                self._last_id_saved = record_id
                processed_since_save += 1

                if processed_since_save >= AUTOSAVE_EVERY:
                    save_checkpoint(self.job_name, record_id)
                    processed_since_save = 0

        except KeyboardInterrupt:
            if self._last_id_saved:
                save_checkpoint(self.job_name, self._last_id_saved)
                print(
                    f"\n[checkpoint] Interrupted. Progress saved at {self._last_id_saved}."
                )
            raise
