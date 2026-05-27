"""
simulation/jobber_user_pool.py

Track which Jobber users are busy on which intervals during a single engine
process lifetime, so jobCreate calls can pick `assignedUsers` without
overlapping a user on themselves.

Jobber has no first-class crew concept. Assignment is per-job. Jobber does
not enforce no-overlap on its end — that policy lives here. State is
process-memory only; restarts lose the busy map (cross-tick overlap is
tolerated as a POC limitation).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from intelligence.logging_config import setup_logging

logger = setup_logging("simulation.jobber_user_pool")


class UserPool:
    """Round-robin pool of Jobber user IDs with per-day overlap tracking.

    The pool itself is fixed at construction time (the list of user IDs
    discovered from tool_ids.json). The per-day busy map grows as `assign`
    records new intervals.
    """

    def __init__(self, user_ids: list[str]):
        self.user_ids: list[str] = list(user_ids)
        # date → { user_id → [(start_dt, end_dt), ...] }
        self._busy: dict[date, dict[str, list[tuple[datetime, datetime]]]] = {}
        self._lru_cursor: int = 0
        self._warned_dates: set[date] = set()

    @property
    def size(self) -> int:
        return len(self.user_ids)

    def assign(
        self,
        start_dt: datetime,
        end_dt: datetime,
        count: int,
    ) -> list[str]:
        """Pick up to `count` users free for [start_dt, end_dt).

        Picks in round-robin order from the LRU cursor so assignments spread
        across the pool. Returns fewer than `count` when too few users are
        free (logs WARN once per day). Returns [] when pool is empty.

        Records the assignment against each picked user before returning, so
        subsequent calls in the same tick see the busy slot.
        """
        if not self.user_ids or count <= 0:
            return []

        day = start_dt.date()
        day_map = self._busy.setdefault(day, {})

        picked: list[str] = []
        n = len(self.user_ids)
        # Scan up to `n` users from the cursor onward.
        for offset in range(n):
            uid = self.user_ids[(self._lru_cursor + offset) % n]
            intervals = day_map.get(uid, [])
            if not any(_overlaps(s, e, start_dt, end_dt) for s, e in intervals):
                picked.append(uid)
                if len(picked) == count:
                    break

        # Advance the cursor by however many slots we consumed (round-robin
        # fairness across subsequent calls).
        self._lru_cursor = (self._lru_cursor + max(1, len(picked))) % n

        for uid in picked:
            day_map.setdefault(uid, []).append((start_dt, end_dt))

        if len(picked) < count and day not in self._warned_dates:
            self._warned_dates.add(day)
            logger.warning(
                "UserPool: requested %d users for %s %s-%s, only %d free",
                count, day.isoformat(),
                start_dt.strftime("%H:%M"), end_dt.strftime("%H:%M"),
                len(picked),
            )

        return picked

    def clear_day(self, d: date) -> None:
        """Reset busy intervals for a specific day (used in tests/long runs)."""
        self._busy.pop(d, None)
        self._warned_dates.discard(d)


def _overlaps(
    a_start: datetime, a_end: datetime,
    b_start: datetime, b_end: datetime,
) -> bool:
    """Half-open interval overlap test: [a_start, a_end) ∩ [b_start, b_end)."""
    return a_start < b_end and b_start < a_end


def load_user_pool_from_config(tool_ids: dict) -> Optional[UserPool]:
    """Construct a UserPool from a loaded tool_ids dict. Returns None if
    the jobber.user_pool key is absent or empty."""
    user_ids = (tool_ids.get("jobber") or {}).get("user_pool") or []
    if not user_ids:
        return None
    return UserPool(user_ids)
