#!/bin/bash
# run_automations.sh
#
# Wrapper for the automation runner — safe to call from cron.
# Loads .env, activates virtualenv if present, then runs --poll.
#
# Cron entry (every 5 minutes):
#   */5 * * * * "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc/run_automations.sh"

set -euo pipefail

PROJECT_DIR="/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
LOG_FILE="$PROJECT_DIR/logs/cron_runner.log"
PYTHON=/usr/bin/python3

# Load .env into the environment
set -a
# shellcheck disable=SC1091
source "$PROJECT_DIR/.env"
set +a

cd "$PROJECT_DIR"

# Activate virtualenv if one exists alongside the project
if [ -f "$PROJECT_DIR/.venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/.venv/bin/activate"
    PYTHON="$PROJECT_DIR/.venv/bin/python"
fi

echo "--- $(date -u '+%Y-%m-%d %H:%M:%S UTC') poll start ---" >> "$LOG_FILE"
"$PYTHON" -m automations.runner --poll >> "$LOG_FILE" 2>&1
echo "--- $(date -u '+%Y-%m-%d %H:%M:%S UTC') poll end ---" >> "$LOG_FILE"
