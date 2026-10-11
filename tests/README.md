# Tests

Unit tests for Python code in `src/` (pytest), run in CI on every pull request.

| File | What it covers |
|---|---|
| `test_frankfurter.py` | FX date window (first run, incremental, single day, up to date, "today" in UTC) and flattening of the API response |
| `test_api_client.py` | HTTP client: success, retries with exponential backoff on 429/5xx and network errors, fail fast on 4xx, invalid JSON |

No network and no Spark session: HTTP calls and `time.sleep` are mocked.
`pyspark` and `delta-spark` are dev dependencies only so that `frankfurter.py` can be imported.

Run from the repo root:

```bash
pytest
```
