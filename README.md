# listing-news-paper

Paper listing/delist watcher. Official public CEX announcement APIs only. No orders.

Each source fetches on its own clock and classifies as soon as that fetch returns. First successful fetch per source is a seed. Later fetches emit only new items.

## Sources

- **Binance listing:** CMS catalog 48
- **Binance delist:** CMS catalog 161
- **Upbit:** 거래 공지. `신규 거래지원` → BUY, `거래지원 종료` → SELL. 유의 종목 is ignored
- **Coinbase status:** incidents; `Markets Open` is `markets_open`
- **Coinbase currencies:** online/delisted snapshot diff as `currency_list_change` (not the same as an official listing announcement)
- **Bybit:** announcement API as-is (`new_crypto` / `delistings`)

## Run

```bash
python3 -m pip install -r requirements.txt
python3 listing_news_paper.py --selftest
python3 -u listing_news_paper.py --once
python3 -u listing_news_paper.py --loops 3 --interval 30
```

Cache defaults to `~/.hermes/cache/listing-news-paper`. Override with `LISTING_NEWS_PAPER_CACHE`.

CLI interval floor is 15 seconds, measured start-to-start per source. One IP. No proxies. A 429 skips that source only.

## Paper contract

- `orders` is always `0`
- no keys, wallets, or order endpoints
- actionable output is `BUY` / `SELL` / `SKIP` on the announcement, not a live fill
- `event_kind` distinguishes listing/delist announcements, Coinbase `markets_open`, and `currency_list_change`

See `docs/SAFETY.md`.
