# simulation/exceptions.py


class TokenExpiredError(Exception):
    """Raised when an API token has expired (HTTP 401)."""


class RateLimitError(Exception):
    """Raised when an API rate limit is hit (HTTP 429)."""


class ToolUnavailableError(Exception):
    """Raised when a tool returns a server error (HTTP 500–504)."""


class ToolAPIError(Exception):
    """Raised for API-level errors from a tool (HTTP 400, 403, 404, etc.)."""
