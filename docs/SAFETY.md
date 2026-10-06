# Safety

This watcher is paper-only.

- No order submission, signing, or trading-account APIs.
- No API keys, wallets, or secrets. Public announcement endpoints only.
- `paper_event()` always sets `"orders": 0` and `"mode": "paper"`.
- First successful fetch per venue is a seed. It records ids and emits nothing.
- HTTP 429 skips that venue for the poll; it does not retry-hammer.
- Interval floor is 15 seconds. Do not add proxies or multi-IP rotation.
- Cache (`seen.json`, `events.jsonl`, Coinbase currency snapshot) is local state, not source.

Do not add a live execution path to this repo.
