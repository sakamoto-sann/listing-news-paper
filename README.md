# listing-news-paper

Paper listing/delist watcher. Official public CEX announcement APIs only. No orders.

First poll seeds each venue. Later polls emit only new items.

## Venues

- **Binance:** CMS catalog 48 上場 / 161 廃止
- **Upbit:** 거래 공지. `신규 거래지원` → BUY, `거래지원 종료` → SELL. 유의 종목 is ignored
- **Coinbase:** blog is Cloudflare-blocked. Use Exchange `currencies` online/delisted diff plus status incidents matching “Markets Open”
- **Bybit:** announcement API as-is (`new_crypto` / `delistings`)

## Run

```bash
python3 listing_news_paper.py --selftest
python3 listing_news_paper.py --once
python3 listing_news_paper.py --loops 3 --interval 30
```

Cache defaults to `~/.hermes/cache/listing-news-paper`. Override with `LISTING_NEWS_PAPER_CACHE`.

Minimum interval is 15 seconds. One IP. No proxies. A 429 skips that venue for the poll.

## Paper contract

- `orders` is always `0`
- no keys, wallets, or order endpoints
- actionable output is `BUY` / `SELL` / `SKIP` on the announcement, not a live fill

See `docs/SAFETY.md`.
