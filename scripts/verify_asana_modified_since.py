import os
import sys
from datetime import date, timedelta
from pathlib import Path

import asana

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from auth import get_client


client = get_client("asana")
tasks_api = asana.TasksApi(client)

PROJECT_GID = "1213719394454339"  # Admin & Operations
FUTURE_DATE = (date.today() + timedelta(days=1)).isoformat() + "T00:00:00Z"

# If modified_since is respected, this must return 0 tasks because nothing
# should be modified in the future.
count = 0
for _ in tasks_api.get_tasks_for_project(
    PROJECT_GID,
    opts={"modified_since": FUTURE_DATE, "opt_fields": "gid", "limit": 100},
):
    count += 1

print(f"Using project gid: {PROJECT_GID}")
print(f"Using modified_since: {FUTURE_DATE}")
print(f"Tasks returned with modified_since=tomorrow: {count}")
print(
    "VERDICT:",
    "parameter IGNORED (returns all tasks)" if count > 0 else "parameter respected",
)
