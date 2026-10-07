"""Unit tests for src/ingestion/api_client.py (get_json).

No real HTTP call and no real waiting happens here:
requests.get is replaced by a mock that returns prepared responses,
time.sleep is replaced by a mock that only records how long we WOULD have waited.
"""
from unittest.mock import Mock, call

import pytest
import requests

from src.ingestion import api_client
from src.ingestion.api_client import RETRYABLE_STATUS, get_json

# Reserved test domain: even by mistake this can never reach a real server
URL = "https://api.example.test/rates"


def make_response(status_code: int, body: str = "{}") -> requests.Response:
    """Build a real requests.Response without any network call."""
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()  # private attribute: the usual way to fake a body
    response.url = URL
    return response


def mock_get(monkeypatch, *outcomes) -> Mock:
    """Make requests.get return (or raise) the given outcomes, one per call."""
    get = Mock(side_effect=list(outcomes))
    monkeypatch.setattr(api_client.requests, "get", get)
    return get


# autouse=True: applied to every test in this file, so no test can ever really sleep
@pytest.fixture(autouse=True)
def mock_sleep(monkeypatch) -> Mock:
    sleep = Mock()
    monkeypatch.setattr(api_client.time, "sleep", sleep)
    return sleep


# ---------- happy path ----------


def test_success_returns_parsed_json(monkeypatch, mock_sleep):
    get = mock_get(monkeypatch, make_response(200, '{"base": "BRL"}'))

    assert get_json(URL, params={"from": "BRL"}) == {"base": "BRL"}

    # One call only, with our params and the default timeout passed through
    get.assert_called_once_with(URL, params={"from": "BRL"}, timeout=30)
    mock_sleep.assert_not_called()


# ---------- transient errors: retry ----------


@pytest.mark.parametrize("status", sorted(RETRYABLE_STATUS))
def test_retryable_status_is_retried_then_succeeds(monkeypatch, mock_sleep, status):
    get = mock_get(monkeypatch, make_response(status), make_response(200, '{"ok": true}'))

    assert get_json(URL) == {"ok": True}

    assert get.call_count == 2
    mock_sleep.assert_called_once_with(2.0)


@pytest.mark.parametrize(
    "error",
    [requests.ConnectionError("connection refused"), requests.Timeout("too slow")],
    ids=["connection_error", "timeout"],
)
def test_network_error_is_retried_then_succeeds(monkeypatch, mock_sleep, error):
    # An exception in side_effect is raised instead of returned
    get = mock_get(monkeypatch, error, make_response(200, '{"ok": true}'))

    assert get_json(URL) == {"ok": True}

    assert get.call_count == 2
    mock_sleep.assert_called_once_with(2.0)


# ---------- giving up ----------


def test_persistent_server_error_raises_after_max_retries(monkeypatch, mock_sleep):
    get = mock_get(monkeypatch, *[make_response(503) for _ in range(3)])

    with pytest.raises(requests.HTTPError):
        get_json(URL)

    assert get.call_count == 3
    # Waited after attempt 1 and 2, but not after the last one
    assert mock_sleep.call_args_list == [call(2.0), call(4.0)]


def test_persistent_network_error_raises_after_max_retries(monkeypatch, mock_sleep):
    get = mock_get(monkeypatch, *[requests.ConnectionError("down") for _ in range(3)])

    with pytest.raises(requests.ConnectionError):
        get_json(URL)

    assert get.call_count == 3
    assert mock_sleep.call_args_list == [call(2.0), call(4.0)]


def test_backoff_doubles_after_each_failed_attempt(monkeypatch, mock_sleep):
    mock_get(monkeypatch, *[make_response(500) for _ in range(4)])

    with pytest.raises(requests.HTTPError):
        get_json(URL, max_retries=4, backoff_seconds=2.0)

    assert mock_sleep.call_args_list == [call(2.0), call(4.0), call(8.0)]


# ---------- errors that must NOT be retried ----------


@pytest.mark.parametrize("status", [400, 401, 404])
def test_client_error_fails_fast_without_retry(monkeypatch, mock_sleep, status):
    get = mock_get(monkeypatch, make_response(status))

    with pytest.raises(requests.HTTPError):
        get_json(URL)

    assert get.call_count == 1
    mock_sleep.assert_not_called()


def test_invalid_json_raises_value_error_with_context(monkeypatch):
    get = mock_get(monkeypatch, make_response(200, "<html>not json</html>"))

    # match = the error message must say where the bad payload came from
    with pytest.raises(ValueError, match="Invalid JSON from"):
        get_json(URL)

    # A malformed payload is not a transient error -> no second attempt
    assert get.call_count == 1