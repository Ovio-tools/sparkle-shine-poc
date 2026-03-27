"""
automations/utils/hubspot_write_lock.py

Per-contact advisory file lock for HubSpot read-modify-write operations.

HubSpot has no atomic increment. If two runner invocations process events for
the same contact concurrently (e.g. cron overlap), both may read the same
counter value before either writes, causing one increment to be silently
discarded. This module provides a per-contact exclusive file lock so only one
process at a time performs a read-modify-write against a given contact's
counters.

Usage:
    from automations.utils.hubspot_write_lock import contact_write_lock

    with contact_write_lock(contact_id):
        contact = hs_client.crm.contacts.basic_api.get_by_id(...)
        # ... compute new values ...
        hs_client.crm.contacts.basic_api.update(...)
"""
import contextlib
import fcntl
import os
import tempfile

_LOCK_DIR = os.path.join(tempfile.gettempdir(), "sparkle_shine_hs_locks")
os.makedirs(_LOCK_DIR, exist_ok=True)


@contextlib.contextmanager
def contact_write_lock(contact_id: str):
    """
    Acquire an exclusive file lock for the given HubSpot contact_id.

    Blocks until the lock is available (i.e. until any concurrent process
    holding the lock for this contact releases it). Releases automatically
    on context exit, even if an exception is raised.
    """
    lock_path = os.path.join(_LOCK_DIR, f"hs_contact_{contact_id}.lock")
    with open(lock_path, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
