"""Workspace-local Python startup tweaks.

Suppresses interpreter/toolchain warnings that come from the local macOS system
Python build rather than from project code. The repo now targets Python 3.11+
for Railway and local dev, so these warnings are just noise while this machine
is still using Python 3.9 + LibreSSL.
"""

from __future__ import annotations

import warnings

warnings.filterwarnings(
    "ignore",
    message=r"You are using a Python version 3\.9 past its end of life.*",
    category=FutureWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"You are using a non-supported Python version \(3\.9\.6\).*",
    category=FutureWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
    category=Warning,
)
