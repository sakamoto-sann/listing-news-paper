#!/usr/bin/env python3
"""Paper listing/delist watcher on official CEX announcement APIs. No orders.

Bybit v5, Binance CMS catalogs 48/161, Upbit notices, Coinbase Exchange
public currencies + status incidents. One IP, polite interval. No proxies.

  python3 listing_news_paper.py --selftest
  python3 listing_news_paper.py --once
  python3 listing_news_paper.py --loops 3 --interval 30

Cache defaults to ~/.hermes/cache/listing-news-paper.
Override with LISTING_NEWS_PAPER_CACHE.

Never prints secrets. Never sends orders.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple


def cache_dir() -> Path:
    raw = os.environ.get("LISTING_NEWS_PAPER_CACHE")
    if raw:
        return Path(raw)
    return Path.home() / ".hermes" / "cache" / "listing-news-paper"


def seen_path() -> Path:
    return cache_dir() / "seen.json"


def log_path() -> Path:
    return cache_dir() / "events.jsonl"


def cb_snap_path() -> Path:
    return cache_dir() / "coinbase_currencies.json"


CTX = ssl.create_default_context()
UA = "listing-news-paper/1.0"
MIN_INTERVAL_SEC = 15.0

BYBIT = "https://api.bybit.com/v5/announcements/index"
BINANCE_CATALOG = (
    "https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query"
)
UPBIT = "https://api-manager.upbit.com/api/v1/announcements"
CB_STATUS = "https://status.exchange.coinbase.com/index.json"
CB_CURRENCIES = "https://api.exchange.coinbase.com/currencies"

PAREN = re.compile(r"\(([A-Z0-9]{2,12})\)")
LISTING_OF = re.compile(
    r"\b(?:upcoming listing|new listing|listing|to list|lists?)\s*(?:of\s+|:\s*)?([A-Z][A-Z0-9]{1,11})\b",
    re.I,
)
DELIST_OF = re.compile(
    r"\bdelist(?:ing)?(?:\s+of)?\s+([A-Z][A-Z0-9]{1,11})\b",
    re.I,
)
PAIR = re.compile(r"\b([A-Z]{2,12})(?:USDT|USDC|-USD)\b")

QUOTE = {
    "USDT",
    "USDC",
    "USD",
    "USDE",
    "FDUSD",
    "BTC",
    "ETH",
    "EUR",
    "KRW",
    "SPOT",
    "PERP",
    "BYBIT",
    "OKX",
    "BINANCE",
    "UPBIT",
    "COINBASE",
}
STOP = QUOTE | {
    "MULTIPLE",
    "NEW",
    "THE",
    "AND",
    "TOKEN",
    "TOKENS",
    "PERPETUAL",
    "CONTRACT",
    "CONTRACTS",
    "NOTICE",
    "REMOVAL",
    "SUPPORT",
    "NETWORK",
    "FUTURES",
    "MARGINED",
}

LIST_KEYS = {"new_crypto", "new_fiat", "latest_buy_crypto"}
DELIST_KEYS = {"delistings", "delist", "delisted"}
LIST_WORDS = (
    "listing",
    "to list",
    "will list",
    "lists ",
    "listed on",
    "will launch",
    "will add",
    "adds ",
    "markets open",
    "full trading",
    "신규 거래지원",
    "마켓 추가",
)
DELIST_WORDS = (
    "delist",
    "delisting",
    "remove from",
    "will be removed",
    "will delist",
    "removal of",
    "거래지원 종료",
    "상장폐지",
)


def http_json(url: str, timeout: int = 15) -> Tuple[int, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as resp:
            raw = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read() if e.fp else b""
        code = e.code
    except urllib.error.URLError as e:
        return 0, {"_error": str(e.reason)[:200]}
    text = raw.decode("utf-8", "replace") if raw else ""
    try:
        parsed = json.loads(text) if text else None
    except json.JSONDecodeError:
        parsed = {"_raw": text[:2000]}
    return code, parsed


def extract_tickers(title: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for m in PAREN.findall(title.upper()):
        if m in STOP or m in seen:
            continue
        seen.add(m)
        out.append(m)
    if out:
        return out
    for rx in (LISTING_OF, DELIST_OF):
        m = rx.search(title)
        if not m:
            continue
        tok = m.group(1).upper()
        if tok in STOP or tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    if out:
        return out
    for m in PAIR.findall(title.upper()):
        if m in STOP or m in seen:
            continue
        seen.add(m)
        out.append(m)
    return out


def classify(title: str, type_key: str) -> str:
    key = (type_key or "").lower()
    t = title.lower()
    if "유의 종목" in title:
        return "SKIP"
    if key in DELIST_KEYS or any(w in t for w in DELIST_WORDS):
        return "SELL"
    if key in LIST_KEYS or any(w in t for w in LIST_WORDS):
        return "BUY"
    return "SKIP"


def load_seen() -> Dict[str, Any]:
    empty: Dict[str, Any] = {"ids": [], "seeded": []}
    path = seen_path()
    if not path.exists():
        return empty
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return empty
    if not isinstance(data, dict):
        return empty
    ids = data.get("ids")
    if not isinstance(ids, list):
        ids = []
    ids = [str(x) for x in ids][-4000:]
    seeded = data.get("seeded")
    if not isinstance(seeded, list):
        seeded = []
    seeded = [str(x) for x in seeded]
    if not seeded and ids:
        seeded = sorted({i.split(":")[0] for i in ids if ":" in i})
    return {"ids": ids, "seeded": seeded}


def save_seen(seen: Dict[str, Any]) -> None:
    cache_dir().mkdir(parents=True, exist_ok=True)
    path = seen_path()
    tmp = path.with_suffix(".json.tmp")
    payload = {"ids": seen["ids"][-4000:], "seeded": seen.get("seeded") or []}
    tmp.write_text(json.dumps(payload, separators=(",", ":")))
    tmp.replace(path)


def item_id(venue: str, item: Dict[str, Any]) -> str:
    url = str(item.get("url") or "")
    ts = str(item.get("publish_ms") or "")
    title = str(item.get("title") or "")
    return "{}:{}:{}".format(venue, url or ts, title)[:240]


def _norm_item(venue: str, title: str, url: str = "", type_key: str = "", publish_ms: Any = None) -> Dict[str, Any]:
    return {
        "venue": venue,
        "title": title,
        "url": url,
        "type_key": type_key,
        "publish_ms": publish_ms,
    }


def fetch_bybit(limit: int = 20) -> List[Dict[str, Any]]:
    q = urllib.parse.urlencode({"locale": "en-US", "limit": str(limit)})
    code, data = http_json("{}?{}".format(BYBIT, q))
    if code == 429:
        raise RuntimeError("Bybit 429")
    if code != 200 or not isinstance(data, dict) or data.get("retCode") != 0:
        raise RuntimeError("Bybit HTTP {} {}".format(code, str(data)[:180]))
    rows = ((data.get("result") or {}).get("list")) or []
    out: List[Dict[str, Any]] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        typ = it.get("type") or {}
        type_key = str(typ.get("key") or "") if isinstance(typ, dict) else ""
        out.append(
            _norm_item(
                "bybit",
                str(it.get("title") or ""),
                str(it.get("url") or ""),
                type_key,
                it.get("publishTime") or it.get("dateTimestamp"),
            )
        )
    return out


def fetch_binance(limit: int = 20) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    catalogs = ((48, "new_crypto"), (161, "delistings"))
    for catalog_id, type_key in catalogs:
        q = urllib.parse.urlencode(
            {"catalogId": str(catalog_id), "pageNo": "1", "pageSize": str(limit)}
        )
        code, data = http_json("{}?{}".format(BINANCE_CATALOG, q))
        if code == 429:
            raise RuntimeError("Binance 429")
        if code != 200 or not isinstance(data, dict) or str(data.get("code")) not in ("000000", "0"):
            raise RuntimeError("Binance HTTP {} {}".format(code, str(data)[:180]))
        arts = ((data.get("data") or {}).get("articles")) or []
        for it in arts:
            if not isinstance(it, dict):
                continue
            code_id = str(it.get("code") or it.get("id") or "")
            url = "https://www.binance.com/en/support/announcement/" + code_id if code_id else ""
            out.append(
                _norm_item(
                    "binance",
                    str(it.get("title") or ""),
                    url,
                    type_key,
                    it.get("releaseDate"),
                )
            )
    return out


def fetch_upbit(limit: int = 20) -> List[Dict[str, Any]]:
    q = urllib.parse.urlencode(
        {"os": "web", "page": "1", "per_page": str(limit), "category": "trade"}
    )
    code, data = http_json("{}?{}".format(UPBIT, q))
    if code == 429:
        raise RuntimeError("Upbit 429")
    if code != 200 or not isinstance(data, dict) or not data.get("success"):
        raise RuntimeError("Upbit HTTP {} {}".format(code, str(data)[:180]))
    notices = ((data.get("data") or {}).get("notices")) or []
    out: List[Dict[str, Any]] = []
    for it in notices:
        if not isinstance(it, dict):
            continue
        nid = str(it.get("id") or "")
        url = "https://upbit.com/service_center/notice?id=" + nid if nid else ""
        out.append(_norm_item("upbit", str(it.get("title") or ""), url, "", it.get("listed_at")))
    return out


def _load_cb_snap() -> Dict[str, str]:
    path = cb_snap_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items()}


def _save_cb_snap(snap: Dict[str, str]) -> None:
    cache_dir().mkdir(parents=True, exist_ok=True)
    path = cb_snap_path()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(snap, separators=(",", ":")))
    tmp.replace(path)


def fetch_coinbase() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    code, data = http_json(CB_STATUS)
    if code == 429:
        raise RuntimeError("Coinbase status 429")
    if code != 200 or not isinstance(data, dict):
        raise RuntimeError("Coinbase status HTTP {} {}".format(code, str(data)[:180]))
    for inc in data.get("incidents") or []:
        if not isinstance(inc, dict):
            continue
        iid = str(inc.get("id") or "")
        url = "https://status.exchange.coinbase.com/incidents/" + iid if iid else CB_STATUS
        out.append(_norm_item("coinbase", str(inc.get("name") or ""), url, "", None))

    code, cur = http_json(CB_CURRENCIES)
    if code == 429:
        raise RuntimeError("Coinbase currencies 429")
    if code != 200 or not isinstance(cur, list):
        raise RuntimeError("Coinbase currencies HTTP {} {}".format(code, str(cur)[:180]))
    now = {}
    for c in cur:
        if not isinstance(c, dict) or not c.get("id"):
            continue
        now[str(c["id"])] = str(c.get("status") or "")
    prev = _load_cb_snap()
    if not prev:
        _save_cb_snap(now)
        return out
    for cid, st in now.items():
        old = prev.get(cid)
        if old is None and st == "online":
            out.append(
                _norm_item(
                    "coinbase",
                    "Coinbase listed ({})".format(cid),
                    "https://api.exchange.coinbase.com/currencies",
                    "new_crypto",
                    None,
                )
            )
        elif old == "online" and st == "delisted":
            out.append(
                _norm_item(
                    "coinbase",
                    "Coinbase delisted ({})".format(cid),
                    "https://api.exchange.coinbase.com/currencies",
                    "delistings",
                    None,
                )
            )
    for cid, st in prev.items():
        if cid not in now and st == "online":
            out.append(
                _norm_item(
                    "coinbase",
                    "Coinbase delisted ({})".format(cid),
                    "https://api.exchange.coinbase.com/currencies",
                    "delistings",
                    None,
                )
            )
    _save_cb_snap(now)
    return out


FETCHERS = (
    ("bybit", fetch_bybit),
    ("binance", fetch_binance),
    ("upbit", fetch_upbit),
    ("coinbase", fetch_coinbase),
)


def fetch_all() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, fn in FETCHERS:
        try:
            rows = fn()
        except RuntimeError as e:
            print("skip {}: {}".format(name, e))
            continue
        print("fetched {} n={}".format(name, len(rows)))
        out.extend(rows)
    return out


def paper_event(item: Dict[str, Any]) -> Dict[str, Any]:
    title = item["title"]
    side = classify(title, item.get("type_key") or "")
    tickers = extract_tickers(title)
    if side != "SKIP" and not tickers:
        return {
            "mode": "paper",
            "orders": 0,
            "action": "SKIP",
            "reason": "no_ticker",
            "venue": item["venue"],
            "title": title,
            "url": item.get("url") or "",
            "type_key": item.get("type_key") or "",
        }
    if side == "SKIP":
        reason = "not_list_or_delist"
    else:
        reason = "listing" if side == "BUY" else "delist"
    return {
        "mode": "paper",
        "orders": 0,
        "action": side,
        "reason": reason,
        "tickers": tickers,
        "venue": item["venue"],
        "title": title,
        "url": item.get("url") or "",
        "type_key": item.get("type_key") or "",
        "publish_ms": item.get("publish_ms"),
    }


def selftest() -> int:
    cases = [
        (
            "Bybit is excited to announce the upcoming listing of Streamflow (STREAM) on our Spot trading platform!",
            "new_crypto",
            "BUY",
            ["STREAM"],
        ),
        (
            "Bybit is excited to announce the listing of PENGU on our Spot trading platform!",
            "new_crypto",
            "BUY",
            ["PENGU"],
        ),
        (
            "New listing: CTUSDT Perpetual Contract, with up to 20x leverage",
            "new_crypto",
            "BUY",
            ["CTUSDT"],
        ),
        (
            "Delisting of BLASTUSDT Perpetual Contract",
            "delistings",
            "SELL",
            ["BLASTUSDT"],
        ),
        (
            "Binance Futures Will Launch USDⓈ-Margined CTUSDT Perpetual Contract (2026-10-01)",
            "new_crypto",
            "BUY",
            ["CT"],
        ),
        (
            "Binance Futures Will Delist Multiple USDⓈ-M Perpetual Contracts (2026-10-05)",
            "delistings",
            "SELL",
            [],
        ),
        (
            "돌핀(POD) 신규 거래지원 안내 (KRW, BTC, USDT 마켓)",
            "",
            "BUY",
            ["POD"],
        ),
        (
            "아이콘(ICX) 거래지원 종료 안내 (10/19 15:00)",
            "",
            "SELL",
            ["ICX"],
        ),
        (
            "블라스트(BLAST) 거래 유의 종목 지정 안내",
            "",
            "SKIP",
            ["BLAST"],
        ),
        (
            "CT-USD Markets Open",
            "",
            "BUY",
            ["CT"],
        ),
        (
            "Coinbase listed (FOO)",
            "new_crypto",
            "BUY",
            ["FOO"],
        ),
        (
            "Maintenance update for Spot",
            "maintenance",
            "SKIP",
            [],
        ),
    ]
    failed = 0
    for title, key, want_side, want_tickers in cases:
        side = classify(title, key)
        ticks = extract_tickers(title)
        ok_side = side == want_side
        if want_side == "SKIP":
            ok_ticks = True
        elif want_tickers == []:
            ok_ticks = ticks == []
        else:
            ok_ticks = ticks == want_tickers
        if not (ok_side and ok_ticks):
            failed += 1
            print("FAIL", title[:70], "side", side, "want", want_side, "ticks", ticks)
    if failed:
        print("selftest failed", failed)
        return 1
    print("selftest ok")
    return 0


def run_once(seen: Dict[str, Any], emit_skip: bool) -> List[Dict[str, Any]]:
    items = fetch_all()
    start_known = set(seen["ids"])
    seeded = set(seen.get("seeded") or [])
    events: List[Dict[str, Any]] = []
    cache_dir().mkdir(parents=True, exist_ok=True)
    seen_venues = {it["venue"] for it in items}
    for item in items:
        venue = item["venue"]
        iid = item_id(venue, item)
        is_new = iid not in start_known
        if is_new:
            seen["ids"].append(iid)
        seeding = venue not in seeded
        if seeding or not is_new:
            continue
        ev = paper_event(item)
        ev["id"] = iid
        ev["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        ev["new"] = True
        if ev["action"] == "SKIP" and not emit_skip:
            continue
        events.append(ev)
        with log_path().open("a") as f:
            f.write(json.dumps(ev, separators=(",", ":")) + "\n")
    for venue in seen_venues:
        if venue not in seeded:
            seeded.add(venue)
    seen["seeded"] = sorted(seeded)
    save_seen(seen)
    return events


def main() -> int:
    p = argparse.ArgumentParser(description="Paper listing/delist watcher. No orders.")
    p.add_argument("--selftest", action="store_true")
    p.add_argument("--once", action="store_true")
    p.add_argument("--loops", type=int, default=1)
    p.add_argument("--interval", type=float, default=30.0)
    p.add_argument("--emit-skip", action="store_true")
    args = p.parse_args()
    if args.selftest:
        return selftest()
    interval = args.interval
    if interval < MIN_INTERVAL_SEC:
        interval = MIN_INTERVAL_SEC
    loops = 1 if args.once else args.loops
    if loops < 1:
        raise SystemExit("loops must be >= 1")
    seen = load_seen()
    last: List[Dict[str, Any]] = []
    for i in range(loops):
        last = run_once(seen, args.emit_skip)
        actionable = [e for e in last if e["action"] in ("BUY", "SELL")]
        print(
            "poll {} new_actionable={} logged={} seen={} seeded={}".format(
                i + 1,
                len(actionable),
                len(last),
                len(seen["ids"]),
                ",".join(seen.get("seeded") or []),
            )
        )
        for e in last:
            print(
                "{ts} {action} {tickers} {venue} {reason} {title}".format(
                    ts=e["ts"],
                    action=e["action"],
                    tickers=",".join(e.get("tickers") or []) or "-",
                    venue=e["venue"],
                    reason=e["reason"],
                    title=e["title"][:100],
                )
            )
        if i + 1 < loops:
            time.sleep(interval)
    print("log {}".format(log_path()))
    print("seen {}".format(seen_path()))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
