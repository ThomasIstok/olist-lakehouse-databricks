"""Minimal reusable HTTP client for JSON REST APIs.

Adds what a plain requests.get() lacks in production:
- timeout: a hanging API never blocks the pipeline forever,
- retries with exponential backoff on transient errors (network, 429, 5xx),
- fail fast on client errors (4xx) - retrying a bad request does not help,
- clear logging of every attempt and a readable error for invalid JSON.
"""
from __future__ import annotations

import logging
import time

import requests

logger = logging.getLogger(__name__)

# Transient HTTP errors worth retrying: rate limit (429) and server-side errors (5xx)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _wait(attempt: int, backoff_seconds: float) -> None:
    """Sleep with exponential backoff: 2s, 4s, 8s ... for attempt 1, 2, 3 ..."""
    delay = backoff_seconds * 2 ** (attempt - 1)
    logger.info("Waiting %.0fs before next attempt", delay)
    time.sleep(delay)


def get_json(
    url: str,
    params: dict | None = None,
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
    timeout_seconds: int = 30,
) -> dict:
    """GET a JSON endpoint, retrying transient failures with exponential backoff.

    Args:
        url: Endpoint URL.
        params: Query string parameters, e.g. {"from": "BRL", "to": "EUR,CZK"}.
        max_retries: Maximum number of attempts.
        backoff_seconds: Base wait time, doubled after each failed attempt.
        timeout_seconds: Max time to wait for a response per attempt.

    Returns:
        Parsed JSON response (raw payload, no transformation).

    Raises:
        requests.HTTPError: Client error (4xx) or server error after all retries.
        requests.ConnectionError / requests.Timeout: Network error after all retries.
        ValueError: Response is not valid JSON.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout_seconds)

            # Transient server-side problem -> try again (unless this was the last attempt)
            if response.status_code in RETRYABLE_STATUS and attempt < max_retries:
                logger.warning(
                    "HTTP %s from %s (attempt %d/%d)", response.status_code, url, attempt, max_retries
                )
                _wait(attempt, backoff_seconds)
                continue

            # Any other error (4xx, or 5xx on last attempt) -> raise immediately
            response.raise_for_status()

        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == max_retries:
                logger.error("Network error calling %s, giving up after %d attempts", url, attempt)
                raise
            logger.warning("Network error calling %s (attempt %d/%d): %s", url, attempt, max_retries, exc)
            _wait(attempt, backoff_seconds)
            continue

        # Success: parse JSON with a readable error if the payload is malformed
        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError(f"Invalid JSON from {response.url}: {response.text[:200]}") from exc

        logger.info("GET %s -> HTTP %s (%d bytes)", response.url, response.status_code, len(response.content))
        return payload

    raise RuntimeError("Unreachable: retry loop exited without result")  # keeps type checkers happy