"""Retry utilities for API calls."""

from __future__ import annotations

import logging

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


def _is_google_transient(exc: BaseException) -> bool:
    """Check if a Google API error is transient (429 or 5xx)."""
    try:
        from googleapiclient.errors import HttpError

        if isinstance(exc, HttpError):
            return exc.resp.status in (429, 500, 502, 503, 504)
    except ImportError:
        pass
    return False


google_retry = retry(
    retry=retry_if_exception(_is_google_transient),
    wait=wait_exponential(multiplier=2, min=4, max=300),
    stop=stop_after_attempt(15),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
