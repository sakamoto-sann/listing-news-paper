# Safety

This watcher is paper-only.

- No order submission, signing, or trading-account APIs.
- No credentials, wallets, or secrets. Public announcement endpoints only.
- `paper_event()` always sets `"orders": 0` and `"mode": "paper"`.
- First successful fetch per **source** (`bybit`, `binance_listing`, `binance_delist`, `upbit`, `coinbase_status`, `coinbase_currencies`) is a seed. It records ids and emits nothing.
- HTTP 429 skips that source for the attempt; other sources still run. It does not retry-hammer. `Retry-After` is honored (capped at 900 seconds). Consecutive errors lengthen the next start-to-start wait (2x, 4x, 8x, 16x interval).
- Interval is start-to-start per source. Floor is 15 seconds in the CLI (default 30). Missed beats are not replayed as a burst.
- One persistent HTTP client for the process. Do not add proxies or multi-IP rotation.
- Binance's signed announcement stream is out of scope here.
- Cache (`seen.json`, `events.jsonl`, `cycles.jsonl`, Coinbase currency snapshot) is local state, not source.
- Cycle logs record `http_ms` and `status_code` even when `fetched=0`. `cycle_elapsed_s` is start-to-finish of that attempt. `start_interval_s` is the gap from the previous start.

Do not add a live execution path to this repo.
