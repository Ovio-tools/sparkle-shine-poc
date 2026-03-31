"""
Abstract base class for all Sparkle & Shine tool syncers.

Every syncer subclass sets class-level tool_name and implements sync().
The base class owns sync_state table creation, last-sync reads, and state writes.
"""
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from database.schema import get_connection
from intelligence.logging_config import setup_logging


# ------------------------------------------------------------------ #
# Result container
# ------------------------------------------------------------------ #

@dataclass
class SyncResult:
    tool_name: str
    records_synced: int
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    is_incremental: bool = False


# ------------------------------------------------------------------ #
# sync_state DDL
# ------------------------------------------------------------------ #

_CREATE_SYNC_STATE = """
CREATE TABLE IF NOT EXISTS sync_state (
    tool_name       TEXT PRIMARY KEY,
    last_sync_at    TEXT NOT NULL,
    records_synced  INTEGER,
    last_error      TEXT
);
"""


# ------------------------------------------------------------------ #
# Base class
# ------------------------------------------------------------------ #

class BaseSyncer(ABC):
    tool_name: str = ""  # overridden by each subclass

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = get_connection(db_path)
        self.logger = setup_logging(f"syncer.{self.tool_name}")
        self._ensure_sync_state_table()

    def _ensure_sync_state_table(self) -> None:
        with self.db:
            self.db.execute(_CREATE_SYNC_STATE)

    @abstractmethod
    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        """Pull data from the tool and upsert into SQLite.

        If since is None, perform a full sync.
        If since is provided, perform an incremental sync (records updated after that time).
        Returns a SyncResult with counts and any non-fatal errors.
        """

    def get_last_sync_time(self) -> Optional[datetime]:
        """Return the last successful sync timestamp, or None if never synced."""
        cursor = self.db.execute(
            "SELECT last_sync_at FROM sync_state WHERE tool_name = %s",
            (self.tool_name,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        try:
            return datetime.fromisoformat(row["last_sync_at"])
        except (ValueError, TypeError):
            return None

    def update_sync_state(self, record_count: int, error: Optional[str] = None) -> None:
        """Write the current UTC timestamp and record count to sync_state."""
        with self.db:
            self.db.execute(
                """
                INSERT INTO sync_state (tool_name, last_sync_at, records_synced, last_error)
                VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
                ON CONFLICT(tool_name) DO UPDATE SET
                    last_sync_at   = CURRENT_TIMESTAMP,
                    records_synced = EXCLUDED.records_synced,
                    last_error     = EXCLUDED.last_error
                """,
                (self.tool_name, record_count, error),
            )

    def close(self) -> None:
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
