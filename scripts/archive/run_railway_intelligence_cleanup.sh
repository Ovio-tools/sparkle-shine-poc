#!/bin/bash
set -euo pipefail

MODE="${1:---dry-run}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_PATH="${SCRIPT_DIR}/cleanup_intelligence_sync_artifacts.sql"

case "$MODE" in
  --dry-run|dry-run)
    APPLY=0
    ;;
  --apply|apply)
    APPLY=1
    ;;
  *)
    echo "Usage: $0 [--dry-run|--apply]" >&2
    exit 1
    ;;
esac

if ! command -v railway >/dev/null 2>&1; then
    echo "railway CLI is required on PATH" >&2
    exit 1
fi

printf "\\set apply %s\n\\\\i '%s'\n\\\\q\n" "$APPLY" "$SQL_PATH" | railway connect Postgres
