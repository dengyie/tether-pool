"""Uvicorn access-log filters for credentials carried in query strings."""

import logging
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


class RedactQueryTokenFilter(logging.Filter):
    """Redact token-like query parameters from Uvicorn's access-log path."""

    _sensitive_keys = {"token", "api_token", "access_token"}

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if not isinstance(args, tuple) or len(args) < 3:
            return True
        path = args[2]
        if not isinstance(path, str) or "?" not in path:
            return True
        try:
            parts = urlsplit(path)
            query = urlencode([
                (key, "[REDACTED]" if key.lower() in self._sensitive_keys else value)
                for key, value in parse_qsl(parts.query, keep_blank_values=True)
            ])
            safe_path = urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
        except (ValueError, UnicodeError):
            # Malformed URLs must not leak a token-shaped parameter in logs.
            safe_path = re.sub(
                r"(?i)([?&](?:token|api_token|access_token)=)[^&#]*",
                r"\1[REDACTED]",
                path,
            )
        record.args = (*args[:2], safe_path, *args[3:])
        return True
