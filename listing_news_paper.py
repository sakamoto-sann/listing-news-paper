#!/usr/bin/env python3
"""Paper listing/delist watcher on official CEX announcement APIs. No orders.

Sources run independently and emit as soon as each fetch completes.
Bybit v5, Binance CMS catalogs 48 and 161, Upbit notices, Coinbase
Exchange status and currencies. One IP, start-to-start interval. No proxies.

  python3 listing_news_paper.py --selftest
  python3 listing_news_paper.py --once
  python3 listing_news_paper.py --loops 3 --interval 30

Cache defaults to ~/.hermes/cache/listing-news-paper.
Override with LISTING_NEWS_PAPER_CACHE.

Never prints secrets. Never sends orders.
Binance's signed announcement stream is out of scope.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

HttpClient = Any


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


def cycle_log_path() -> Path:
    return cache_dir() / "cycles.jsonl"


UA = "listing-news-paper/1.0"
MIN_INTERVAL_SEC = 15.0
HTTP_TIMEOUT_SEC = 15.0
RETRY_AFTER_PAUSE_S = 86400.0
MAX_ERROR_BACKOFF_FACTOR = 16

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

_OUT_LOCK = threading.Lock()
_SEEN_IO_LOCK = threading.Lock()


@dataclass(frozen=True)
class FetchResult:
    source_id: str
    items: List[Dict[str, Any]]
    status_code: Optional[int] = None
    http_ms: Optional[float] = None
    received_at: Optional[float] = None
    error: Optional[str] = None
    retry_after_s: Optional[float] = None


def make_client() -> HttpClient:
    try:
        import httpx
    except ImportError as e:
        raise SystemExit("httpx is required: pip install -r requirements.txt") from e
    limits = httpx.Limits(
        max_connections=8,
        max_keepalive_connections=8,
        keepalive_expiry=60.0,
    )
    return httpx.Client(
        headers={"User-Agent": UA, "Accept": "application/json"},
        timeout=HTTP_TIMEOUT_SEC,
        limits=limits,
        follow_redirects=True,
    )


def _http_ms(t0: Any, t1: Any) -> Optional[float]:
    if isinstance(t0, (int, float)) and isinstance(t1, (int, float)):
        return round((float(t1) - float(t0)) * 1000.0, 3)
    return None


def parse_retry_after(value: Any, now: Optional[float] = None) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return float(int(text))
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError, IndexError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if now is None:
        now = time.time()
    return max(0.0, dt.timestamp() - float(now))


def should_pause_retry_after(retry_after_s: Optional[float]) -> bool:
    return retry_after_s is not None and float(retry_after_s) >= RETRY_AFTER_PAUSE_S


def schedule_next_due(
    started: float,
    received_at: float,
    interval: float,
    consecutive_errors: int,
    retry_after_s: Optional[float],
) -> float:
    due = started + interval
    if consecutive_errors > 0:
        factor = min(2 ** min(consecutive_errors, 4), MAX_ERROR_BACKOFF_FACTOR)
        due = max(due, started + interval * factor)
    if retry_after_s is not None and not should_pause_retry_after(retry_after_s):
        due = max(due, received_at + max(0.0, float(retry_after_s)))
    return due


def http_json(
    client: HttpClient, url: str, timeout: float = HTTP_TIMEOUT_SEC
) -> Tuple[int, Any, float, float, Optional[float]]:
    t0 = time.monotonic()
    retry_after_s = None
    try:
        resp = client.get(url, timeout=timeout)
        t1 = time.monotonic()
        code = int(getattr(resp, "status_code", 0) or 0)
        text = getattr(resp, "text", None) or ""
        headers = getattr(resp, "headers", None)
        if headers is not None:
            retry_after_s = parse_retry_after(headers.get("Retry-After"))
    except Exception as e:
        t1 = time.monotonic()
        return 0, {"_error": str(e)[:200]}, t0, t1, None
    try:
        parsed = json.loads(text) if text else None
    except json.JSONDecodeError:
        parsed = {"_raw": text[:2000]}
    return code, parsed, t0, t1, retry_after_s


def _http_error(
    source_id: str,
    code: int,
    t0: float,
    t1: float,
    message: str,
    retry_after_s: Optional[float] = None,
) -> FetchResult:
    return FetchResult(
        source_id=source_id,
        items=[],
        status_code=code,
        http_ms=_http_ms(t0, t1),
        received_at=t1,
        error=message,
        retry_after_s=None if retry_after_s is None else max(0.0, float(retry_after_s)),
    )


def _http_ok(
    source_id: str,
    items: List[Dict[str, Any]],
    code: int,
    t0: float,
    t1: float,
) -> FetchResult:
    return FetchResult(
        source_id=source_id,
        items=items,
        status_code=code,
        http_ms=_http_ms(t0, t1),
        received_at=t1,
        error=None,
        retry_after_s=None,
    )


def coerce_fetch_result(source_id: str, raw: Any) -> FetchResult:
    if isinstance(raw, FetchResult):
        if raw.source_id:
            return raw
        return FetchResult(
            source_id=source_id,
            items=list(raw.items),
            status_code=raw.status_code,
            http_ms=raw.http_ms,
            received_at=raw.received_at,
            error=raw.error,
            retry_after_s=raw.retry_after_s,
        )
    if isinstance(raw, list):
        http_ms = None
        received_at = None
        if raw:
            t0 = raw[0].get("t_http_start")
            t1 = raw[0].get("t_http_end")
            http_ms = _http_ms(t0, t1)
            if isinstance(t1, (int, float)):
                received_at = float(t1)
        return FetchResult(
            source_id=source_id,
            items=raw,
            status_code=None,
            http_ms=http_ms,
            received_at=received_at,
            error=None,
            retry_after_s=None,
        )
    raise TypeError("fetch must return FetchResult or list")


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


def infer_event_kind(item: Dict[str, Any]) -> str:
    kind = str(item.get("event_kind") or "").strip()
    if kind:
        return kind
    key = str(item.get("type_key") or "").lower()
    if key in DELIST_KEYS:
        return "delist_announcement"
    if key in LIST_KEYS:
        return "listing_announcement"
    title = str(item.get("title") or "")
    side = classify(title, key)
    if side == "SELL":
        return "delist_announcement"
    if side == "BUY":
        return "listing_announcement"
    return "other"


def publish_time_utc(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        if n > 1e14:
            n = n / 1e9
        elif n > 1e11:
            n = n / 1e3
        try:
            dt = datetime.fromtimestamp(n, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return publish_time_utc(int(text))
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class SeenState:
    def __init__(self, ids: Optional[Sequence[str]] = None, seeded: Optional[Sequence[str]] = None) -> None:
        self._lock = threading.Lock()
        raw_ids = [str(x) for x in (ids or [])][-4000:]
        self.ids = list(raw_ids)
        self._idset = set(raw_ids)
        self.seeded = set(str(x) for x in (seeded or []))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SeenState":
        ids = data.get("ids") if isinstance(data, dict) else None
        seeded = data.get("seeded") if isinstance(data, dict) else None
        if not isinstance(ids, list):
            ids = []
        if not isinstance(seeded, list):
            seeded = []
        return cls(ids, migrate_seeded(seeded))

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {"ids": list(self.ids)[-4000:], "seeded": sorted(self.seeded)}

    def remember(self, iid: str) -> bool:
        with self._lock:
            if iid in self._idset:
                return False
            self._idset.add(iid)
            self.ids.append(iid)
            if len(self.ids) > 4000:
                self.ids = self.ids[-4000:]
                self._idset = set(self.ids)
            return True

    def is_seeded(self, source_id: str) -> bool:
        with self._lock:
            return source_id in self.seeded

    def mark_seeded(self, source_id: str) -> None:
        with self._lock:
            self.seeded.add(source_id)


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
    return {"ids": ids, "seeded": migrate_seeded(seeded)}


def save_seen(seen: Dict[str, Any]) -> None:
    cache_dir().mkdir(parents=True, exist_ok=True)
    path = seen_path()
    tmp = path.with_suffix(".json.tmp")
    payload = {"ids": seen["ids"][-4000:], "seeded": seen.get("seeded") or []}
    with _SEEN_IO_LOCK:
        tmp.write_text(json.dumps(payload, separators=(",", ":")))
        tmp.replace(path)


def item_id(venue: str, item: Dict[str, Any]) -> str:
    url = str(item.get("url") or "")
    ts = str(item.get("publish_ms") or "")
    title = str(item.get("title") or "")
    return "{}:{}:{}".format(venue, url or ts, title)[:240]


def _norm_item(
    venue: str,
    title: str,
    url: str = "",
    type_key: str = "",
    publish_ms: Any = None,
    event_kind: str = "",
    source_id: str = "",
    t_http_start: Optional[float] = None,
    t_http_end: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "venue": venue,
        "title": title,
        "url": url,
        "type_key": type_key,
        "publish_ms": publish_ms,
        "event_kind": event_kind,
        "source_id": source_id,
        "t_http_start": t_http_start,
        "t_http_end": t_http_end,
    }


def fetch_bybit(client: HttpClient, limit: int = 20) -> FetchResult:
    q = urllib.parse.urlencode({"locale": "en-US", "limit": str(limit)})
    code, data, t0, t1, retry_after_s = http_json(client, "{}?{}".format(BYBIT, q))
    if code == 429:
        return _http_error("bybit", code, t0, t1, "Bybit 429", retry_after_s)
    if code != 200 or not isinstance(data, dict) or data.get("retCode") != 0:
        return _http_error(
            "bybit",
            code,
            t0,
            t1,
            "Bybit HTTP {} {}".format(code, str(data)[:180]),
            retry_after_s,
        )
    rows = ((data.get("result") or {}).get("list")) or []
    out: List[Dict[str, Any]] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        typ = it.get("type") or {}
        type_key = str(typ.get("key") or "") if isinstance(typ, dict) else ""
        kind = "delist_announcement" if type_key in DELIST_KEYS else "listing_announcement"
        if type_key not in LIST_KEYS and type_key not in DELIST_KEYS:
            kind = ""
        out.append(
            _norm_item(
                "bybit",
                str(it.get("title") or ""),
                str(it.get("url") or ""),
                type_key,
                it.get("publishTime") or it.get("dateTimestamp"),
                kind,
                "bybit",
                t0,
                t1,
            )
        )
    return _http_ok("bybit", out, code, t0, t1)


def _fetch_binance_catalog(
    client: HttpClient,
    catalog_id: int,
    type_key: str,
    source_id: str,
    event_kind: str,
    limit: int = 20,
) -> FetchResult:
    q = urllib.parse.urlencode(
        {"catalogId": str(catalog_id), "pageNo": "1", "pageSize": str(limit)}
    )
    code, data, t0, t1, retry_after_s = http_json(client, "{}?{}".format(BINANCE_CATALOG, q))
    if code == 429:
        return _http_error(source_id, code, t0, t1, "Binance {} 429".format(source_id), retry_after_s)
    if code != 200 or not isinstance(data, dict) or str(data.get("code")) not in ("000000", "0"):
        return _http_error(
            source_id,
            code,
            t0,
            t1,
            "Binance {} HTTP {} {}".format(source_id, code, str(data)[:180]),
            retry_after_s,
        )
    arts = ((data.get("data") or {}).get("articles")) or []
    out: List[Dict[str, Any]] = []
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
                event_kind,
                source_id,
                t0,
                t1,
            )
        )
    return _http_ok(source_id, out, code, t0, t1)


def fetch_binance_listing(client: HttpClient, limit: int = 20) -> FetchResult:
    return _fetch_binance_catalog(client, 48, "new_crypto", "binance_listing", "listing_announcement", limit)


def fetch_binance_delist(client: HttpClient, limit: int = 20) -> FetchResult:
    return _fetch_binance_catalog(client, 161, "delistings", "binance_delist", "delist_announcement", limit)


def fetch_upbit(client: HttpClient, limit: int = 20) -> FetchResult:
    q = urllib.parse.urlencode(
        {"os": "web", "page": "1", "per_page": str(limit), "category": "trade"}
    )
    code, data, t0, t1, retry_after_s = http_json(client, "{}?{}".format(UPBIT, q))
    if code == 429:
        return _http_error("upbit", code, t0, t1, "Upbit 429", retry_after_s)
    if code != 200 or not isinstance(data, dict) or not data.get("success"):
        return _http_error(
            "upbit",
            code,
            t0,
            t1,
            "Upbit HTTP {} {}".format(code, str(data)[:180]),
            retry_after_s,
        )
    notices = ((data.get("data") or {}).get("notices")) or []
    out: List[Dict[str, Any]] = []
    for it in notices:
        if not isinstance(it, dict):
            continue
        nid = str(it.get("id") or "")
        url = "https://upbit.com/service_center/notice?id=" + nid if nid else ""
        out.append(
            _norm_item(
                "upbit",
                str(it.get("title") or ""),
                url,
                "",
                it.get("listed_at"),
                "",
                "upbit",
                t0,
                t1,
            )
        )
    return _http_ok("upbit", out, code, t0, t1)


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


def fetch_coinbase_status(client: HttpClient) -> FetchResult:
    code, data, t0, t1, retry_after_s = http_json(client, CB_STATUS)
    if code == 429:
        return _http_error("coinbase_status", code, t0, t1, "Coinbase status 429", retry_after_s)
    if code != 200 or not isinstance(data, dict):
        return _http_error(
            "coinbase_status",
            code,
            t0,
            t1,
            "Coinbase status HTTP {} {}".format(code, str(data)[:180]),
            retry_after_s,
        )
    out: List[Dict[str, Any]] = []
    for inc in data.get("incidents") or []:
        if not isinstance(inc, dict):
            continue
        iid = str(inc.get("id") or "")
        url = "https://status.exchange.coinbase.com/incidents/" + iid if iid else CB_STATUS
        title = str(inc.get("name") or "")
        kind = "markets_open" if "markets open" in title.lower() else "status_incident"
        out.append(
            _norm_item(
                "coinbase",
                title,
                url,
                "",
                inc.get("updated_at") or inc.get("created_at"),
                kind,
                "coinbase_status",
                t0,
                t1,
            )
        )
    return _http_ok("coinbase_status", out, code, t0, t1)


def fetch_coinbase_currencies(client: HttpClient) -> FetchResult:
    code, cur, t0, t1, retry_after_s = http_json(client, CB_CURRENCIES)
    if code == 429:
        return _http_error(
            "coinbase_currencies", code, t0, t1, "Coinbase currencies 429", retry_after_s
        )
    if code != 200 or not isinstance(cur, list):
        return _http_error(
            "coinbase_currencies",
            code,
            t0,
            t1,
            "Coinbase currencies HTTP {} {}".format(code, str(cur)[:180]),
            retry_after_s,
        )
    now = {}
    for c in cur:
        if not isinstance(c, dict) or not c.get("id"):
            continue
        now[str(c["id"])] = str(c.get("status") or "")
    prev = _load_cb_snap()
    if not prev:
        _save_cb_snap(now)
        return _http_ok("coinbase_currencies", [], code, t0, t1)
    out: List[Dict[str, Any]] = []
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
                    "currency_list_change",
                    "coinbase_currencies",
                    t0,
                    t1,
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
                    "currency_list_change",
                    "coinbase_currencies",
                    t0,
                    t1,
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
                    "currency_list_change",
                    "coinbase_currencies",
                    t0,
                    t1,
                )
            )
    _save_cb_snap(now)
    return _http_ok("coinbase_currencies", out, code, t0, t1)


@dataclass(frozen=True)
class Source:
    source_id: str
    venue: str
    fetch: Callable[[HttpClient], Any]


SOURCES: Tuple[Source, ...] = (
    Source("bybit", "bybit", fetch_bybit),
    Source("binance_listing", "binance", fetch_binance_listing),
    Source("binance_delist", "binance", fetch_binance_delist),
    Source("upbit", "upbit", fetch_upbit),
    Source("coinbase_status", "coinbase", fetch_coinbase_status),
    Source("coinbase_currencies", "coinbase", fetch_coinbase_currencies),
)

VENUE_SEED_ALIASES = {
    "binance": ("binance_listing", "binance_delist"),
    "coinbase": ("coinbase_status", "coinbase_currencies"),
}


def migrate_seeded(seeded: Sequence[str]) -> List[str]:
    out = set()
    for name in seeded:
        aliases = VENUE_SEED_ALIASES.get(str(name))
        if aliases:
            out.update(aliases)
        else:
            out.add(str(name))
    return sorted(out)


def paper_event(item: Dict[str, Any]) -> Dict[str, Any]:
    title = item["title"]
    side = classify(title, item.get("type_key") or "")
    tickers = extract_tickers(title)
    kind = infer_event_kind(item)
    t_judge = time.monotonic()
    t0 = item.get("t_http_start")
    t1 = item.get("t_http_end")
    http_ms = _http_ms(t0, t1)
    judge_ms = None
    if isinstance(t1, (int, float)):
        judge_ms = round((t_judge - float(t1)) * 1000.0, 3)
    if side != "SKIP" and not tickers:
        reason = "no_ticker"
        side = "SKIP"
    elif side == "SKIP":
        reason = "not_list_or_delist"
    else:
        reason = "listing" if side == "BUY" else "delist"
    return {
        "mode": "paper",
        "orders": 0,
        "action": side,
        "reason": reason,
        "event_kind": kind,
        "tickers": tickers,
        "venue": item["venue"],
        "source_id": item.get("source_id") or item["venue"],
        "title": title,
        "url": item.get("url") or "",
        "type_key": item.get("type_key") or "",
        "publish_ms": item.get("publish_ms"),
        "publish_time_utc": publish_time_utc(item.get("publish_ms")),
        "timing": {"http_ms": http_ms, "judge_ms": judge_ms, "emit_ms": None},
    }


def _print(msg: str) -> None:
    with _OUT_LOCK:
        print(msg, flush=True)


def _log_event(ev: Dict[str, Any]) -> None:
    cache_dir().mkdir(parents=True, exist_ok=True)
    line = json.dumps(ev, separators=(",", ":")) + "\n"
    with _OUT_LOCK:
        with log_path().open("a") as f:
            f.write(line)


def _log_cycle(rec: Dict[str, Any]) -> None:
    cache_dir().mkdir(parents=True, exist_ok=True)
    line = json.dumps(rec, separators=(",", ":")) + "\n"
    with _OUT_LOCK:
        with cycle_log_path().open("a") as f:
            f.write(line)


def ingest_source(
    source_id: str,
    items: Sequence[Dict[str, Any]],
    seen: SeenState,
    emit_skip: bool,
    on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    seeding = not seen.is_seeded(source_id)
    for item in items:
        venue = str(item.get("venue") or "")
        iid = item_id(venue, item)
        is_new = seen.remember(iid)
        if seeding or not is_new:
            continue
        ev = paper_event(item)
        ev["id"] = iid
        ev["ts"] = utc_now()
        ev["new"] = True
        ev["source_id"] = source_id
        if ev["action"] == "SKIP" and not emit_skip:
            continue
        t_out0 = time.monotonic()
        _print(
            "{ts} {action} {tickers} {venue} {source} {kind} {reason} {title}".format(
                ts=ev["ts"],
                action=ev["action"],
                tickers=",".join(ev.get("tickers") or []) or "-",
                venue=ev["venue"],
                source=source_id,
                kind=ev.get("event_kind") or "-",
                reason=ev["reason"],
                title=ev["title"][:100],
            )
        )
        notify_error = None
        try:
            if on_event is not None:
                on_event(ev)
        except Exception as e:
            notify_error = str(e)[:200]
        timing = ev.setdefault("timing", {})
        timing["emit_ms"] = round((time.monotonic() - t_out0) * 1000.0, 3)
        if notify_error:
            ev["notify_error"] = notify_error
        events.append(ev)
        _log_event(ev)
    if seeding:
        seen.mark_seeded(source_id)
    save_seen(seen.to_dict())
    return events


def _safe_fetch(source: Source, client: HttpClient) -> FetchResult:
    try:
        return coerce_fetch_result(source.source_id, source.fetch(client))
    except Exception as e:
        msg = str(e)[:200]
        status = 429 if "429" in msg else None
        return FetchResult(
            source_id=source.source_id,
            items=[],
            status_code=status,
            http_ms=None,
            received_at=time.monotonic(),
            error=msg,
            retry_after_s=None,
        )


def _cycle_record(
    *,
    source_id: str,
    venue: str,
    result: FetchResult,
    fetched: int,
    new_actionable: int,
    logged: int,
    seeded: bool,
    cycle_elapsed_s: float,
    start_interval_s: Optional[float],
    consecutive_errors: int,
    next_wait_s: Optional[float],
    paused: bool = False,
    notify_errors: int = 0,
) -> Dict[str, Any]:
    return {
        "kind": "cycle",
        "ts": utc_now(),
        "source_id": source_id,
        "venue": venue,
        "fetched": fetched,
        "new_actionable": new_actionable,
        "logged": logged,
        "status_code": result.status_code,
        "http_ms": result.http_ms,
        "error": result.error,
        "retry_after_s": result.retry_after_s,
        "seeded": int(seeded),
        "cycle_elapsed_s": round(cycle_elapsed_s, 3),
        "start_interval_s": None if start_interval_s is None else round(start_interval_s, 3),
        "consecutive_errors": consecutive_errors,
        "next_wait_s": None if next_wait_s is None else round(next_wait_s, 3),
        "paused": int(paused),
        "notify_errors": notify_errors,
    }


def run_scheduler(
    *,
    loops: int,
    interval: float,
    emit_skip: bool,
    sources: Optional[Sequence[Source]] = None,
    client: HttpClient = None,
    seen: Optional[SeenState] = None,
    on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    srcs = list(sources or SOURCES)
    state = seen if seen is not None else SeenState.from_dict(load_seen())
    remaining = {s.source_id: loops for s in srcs}
    next_due = {s.source_id: 0.0 for s in srcs}
    last_started: Dict[str, float] = {}
    consecutive_errors = {s.source_id: 0 for s in srcs}
    in_flight: Dict[str, Tuple[Future, float, Source, Optional[float]]] = {}
    all_events: List[Dict[str, Any]] = []
    workers = max(1, len(srcs))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        while True:
            now = time.monotonic()
            for s in srcs:
                if remaining[s.source_id] <= 0 or s.source_id in in_flight:
                    continue
                if now >= next_due[s.source_id]:
                    prev = last_started.get(s.source_id)
                    start_interval = None if prev is None else now - prev
                    fut = ex.submit(_safe_fetch, s, client)
                    in_flight[s.source_id] = (fut, now, s, start_interval)
                    last_started[s.source_id] = now
                    remaining[s.source_id] -= 1
                    next_due[s.source_id] = now + interval
            if not in_flight:
                dues = [next_due[s.source_id] for s in srcs if remaining[s.source_id] > 0]
                if not dues:
                    break
                time.sleep(max(0.0, min(dues) - time.monotonic()))
                continue
            schedulable = [
                s
                for s in srcs
                if remaining[s.source_id] > 0 and s.source_id not in in_flight
            ]
            timeout = None
            if schedulable:
                timeout = max(0.0, min(next_due[s.source_id] for s in schedulable) - time.monotonic())
            done, _ = wait(
                [t[0] for t in in_flight.values()],
                return_when=FIRST_COMPLETED,
                timeout=timeout,
            )
            if not done:
                continue
            finished = []
            for sid, (fut, started, src, start_interval) in list(in_flight.items()):
                if fut in done:
                    finished.append((sid, fut, started, src, start_interval))
            for sid, fut, started, src, start_interval in finished:
                del in_flight[sid]
                result = fut.result()
                received_at = result.received_at if result.received_at is not None else time.monotonic()
                cycle_elapsed_s = time.monotonic() - started
                next_wait_s = None
                if result.error:
                    consecutive_errors[sid] += 1
                    paused = should_pause_retry_after(result.retry_after_s)
                    next_wait_s = None
                    if paused:
                        remaining[sid] = 0
                    else:
                        due = schedule_next_due(
                            started,
                            received_at,
                            interval,
                            consecutive_errors[sid],
                            result.retry_after_s,
                        )
                        next_due[sid] = due
                        next_wait_s = due - started
                    rec = _cycle_record(
                        source_id=sid,
                        venue=src.venue,
                        result=result,
                        fetched=len(result.items),
                        new_actionable=0,
                        logged=0,
                        seeded=state.is_seeded(sid),
                        cycle_elapsed_s=cycle_elapsed_s,
                        start_interval_s=start_interval,
                        consecutive_errors=consecutive_errors[sid],
                        next_wait_s=next_wait_s,
                        paused=paused,
                    )
                    _log_cycle(rec)
                    _print(
                        "source {} fetched=0 new_actionable=0 logged=0 http_ms={} status={} seeded={} cycle_elapsed_s={} start_interval_s={} error={}".format(
                            sid,
                            rec["http_ms"],
                            rec["status_code"],
                            rec["seeded"],
                            rec["cycle_elapsed_s"],
                            rec["start_interval_s"] if rec["start_interval_s"] is not None else "-",
                            result.error,
                        )
                    )
                    continue
                consecutive_errors[sid] = 0
                events = ingest_source(sid, result.items, state, emit_skip, on_event)
                all_events.extend(events)
                actionable = [e for e in events if e["action"] in ("BUY", "SELL")]
                rec = _cycle_record(
                    source_id=sid,
                    venue=src.venue,
                    result=result,
                    fetched=len(result.items),
                    new_actionable=len(actionable),
                    logged=len(events),
                    seeded=state.is_seeded(sid),
                    cycle_elapsed_s=time.monotonic() - started,
                    start_interval_s=start_interval,
                    consecutive_errors=0,
                    next_wait_s=interval,
                    notify_errors=sum(1 for e in events if e.get("notify_error")),
                )
                _log_cycle(rec)
                _print(
                    "source {} fetched={} new_actionable={} logged={} http_ms={} status={} seeded={} cycle_elapsed_s={} start_interval_s={}".format(
                        sid,
                        rec["fetched"],
                        rec["new_actionable"],
                        rec["logged"],
                        rec["http_ms"],
                        rec["status_code"],
                        rec["seeded"],
                        rec["cycle_elapsed_s"],
                        rec["start_interval_s"] if rec["start_interval_s"] is not None else "-",
                    )
                )
    return all_events


def run_once(
    seen: Dict[str, Any],
    emit_skip: bool,
    sources: Optional[Sequence[Source]] = None,
    client: HttpClient = None,
    on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    own_client = False
    if client is None and sources is None:
        client = make_client()
        own_client = True
    try:
        state = SeenState.from_dict(seen)
        events = run_scheduler(
            loops=1,
            interval=0.0,
            emit_skip=emit_skip,
            sources=sources,
            client=client,
            seen=state,
            on_event=on_event,
        )
        snap = state.to_dict()
        seen.clear()
        seen.update(snap)
        return events
    finally:
        if own_client:
            close = getattr(client, "close", None)
            if callable(close):
                close()


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
    client = make_client()
    try:
        seen = SeenState.from_dict(load_seen())
        run_scheduler(
            loops=loops,
            interval=interval,
            emit_skip=args.emit_skip,
            client=client,
            seen=seen,
        )
        snap = seen.to_dict()
        _print(
            "done seen={} seeded={}".format(
                len(snap["ids"]),
                ",".join(snap.get("seeded") or []),
            )
        )
        _print("log {}".format(log_path()))
        _print("cycles {}".format(cycle_log_path()))
        _print("seen {}".format(seen_path()))
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
