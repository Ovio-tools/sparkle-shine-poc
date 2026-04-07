#!/bin/bash
# Wrapper script for Railway worker service.
# Avoids passing `-m` directly in the start command, which Nixpacks 1.38.0
# misinterprets as a build CLI flag (see: deployment failures 2026-04-07).
exec python -m services.token_keeper
