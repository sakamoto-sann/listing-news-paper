#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import listing_news_paper as m  # noqa: E402


def _item(
    venue: str,
    title: str,
    url: str,
    type_key: str = "new_crypto",
    event_kind: str = "",
    source_id: str = "",
    publish_ms=1,
) -> dict:
    return {
        "venue": venue,
        "title": title,
        "url": url,
        "type_key": type_key,
        "event_kind": event_kind,
        "source_id": source_id or venue,
        "publish_ms": publish_ms,
    }


def _source(source_id: str, venue: str, fetch) -> m.Source:
    return m.Source(source_id, venue, fetch)


class ClassifyTests(unittest.TestCase):
    def test_selftest(self) -> None:
        self.assertEqual(m.selftest(), 0)

    def test_upbit_caution_skip(self) -> None:
        self.assertEqual(m.classify("블라스트(BLAST) 거래 유의 종목 지정 안내", ""), "SKIP")

    def test_paper_event_never_orders(self) -> None:
        ev = m.paper_event(
            {
                "venue": "binance",
                "title": "Binance Will List FOO (FOO)",
                "url": "https://example.com/foo",
                "type_key": "new_crypto",
                "publish_ms": 1,
                "source_id": "binance_listing",
            }
        )
        self.assertEqual(ev["orders"], 0)
        self.assertEqual(ev["mode"], "paper")
        self.assertEqual(ev["action"], "BUY")
        self.assertEqual(ev["tickers"], ["FOO"])
        self.assertEqual(ev["event_kind"], "listing_announcement")

    def test_coinbase_event_kinds(self) -> None:
        listed = m.paper_event(
            _item(
                "coinbase",
                "Coinbase listed (FOO)",
                "https://example.com/c",
                "new_crypto",
                "currency_list_change",
                "coinbase_currencies",
            )
        )
        opened = m.paper_event(
            _item(
                "coinbase",
                "CT-USD Markets Open",
                "https://example.com/m",
                "",
                "markets_open",
                "coinbase_status",
            )
        )
        self.assertEqual(listed["event_kind"], "currency_list_change")
        self.assertEqual(listed["action"], "BUY")
        self.assertEqual(opened["event_kind"], "markets_open")
        self.assertEqual(opened["action"], "BUY")

    def test_publish_time_utc_normalizes(self) -> None:
        self.assertEqual(m.publish_time_utc(1_700_000_000_000), "2023-11-14T22:13:20.000Z")
        self.assertEqual(m.publish_time_utc("2026-10-01T12:00:00+09:00"), "2026-10-01T03:00:00.000Z")
        self.assertIsNone(m.publish_time_utc(None))


class SeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["LISTING_NEWS_PAPER_CACHE"] = self.tmp.name
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: os.environ.pop("LISTING_NEWS_PAPER_CACHE", None))

    def test_first_poll_seeds_and_emits_nothing(self) -> None:
        items = {
            "binance_listing": [_item("binance", "Will List FOO (FOO)", "https://example.com/b1")],
            "bybit": [_item("bybit", "upcoming listing of BAR (BAR)", "https://example.com/y1")],
            "upbit": [_item("upbit", "돌핀(POD) 신규 거래지원 안내", "https://example.com/u1")],
            "coinbase_currencies": [
                _item(
                    "coinbase",
                    "Coinbase listed (FOO)",
                    "https://example.com/c1",
                    "new_crypto",
                    "currency_list_change",
                    "coinbase_currencies",
                )
            ],
        }
        sources = [
            _source(sid, rows[0]["venue"], lambda _c, rows=rows: list(rows))
            for sid, rows in items.items()
        ]
        seen = {"ids": [], "seeded": []}
        events = m.run_once(seen, emit_skip=True, sources=sources)
        self.assertEqual(events, [])
        self.assertEqual(seen["seeded"], ["binance_listing", "bybit", "coinbase_currencies", "upbit"])
        self.assertEqual(len(seen["ids"]), 4)
        saved = json.loads(Path(self.tmp.name, "seen.json").read_text())
        self.assertEqual(saved["seeded"], ["binance_listing", "bybit", "coinbase_currencies", "upbit"])

    def test_second_poll_emits_only_new(self) -> None:
        first = [_item("binance", "Will List FOO (FOO)", "https://example.com/b1")]
        second = first + [_item("binance", "Will List BAZ (BAZ)", "https://example.com/b2")]
        state = {"items": first}

        def fetch(_client):
            return list(state["items"])

        sources = [_source("binance_listing", "binance", fetch)]
        seen = {"ids": [], "seeded": []}
        self.assertEqual(m.run_once(seen, emit_skip=False, sources=sources), [])
        state["items"] = second
        events = m.run_once(seen, emit_skip=False, sources=sources)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["action"], "BUY")
        self.assertEqual(events[0]["tickers"], ["BAZ"])
        self.assertEqual(events[0]["orders"], 0)
        self.assertEqual(events[0]["source_id"], "binance_listing")

    def test_seed_is_per_source_not_venue(self) -> None:
        seen = m.SeenState()
        listing = [_item("binance", "Will List FOO (FOO)", "https://example.com/b1")]
        delist = [
            _item(
                "binance",
                "Will Delist QUX (QUX)",
                "https://example.com/d1",
                "delistings",
                "delist_announcement",
                "binance_delist",
            )
        ]
        self.assertEqual(m.ingest_source("binance_listing", listing, seen, True), [])
        self.assertTrue(seen.is_seeded("binance_listing"))
        self.assertFalse(seen.is_seeded("binance_delist"))
        self.assertEqual(m.ingest_source("binance_delist", delist, seen, True), [])
        self.assertTrue(seen.is_seeded("binance_delist"))

    def test_migrates_old_venue_seeds(self) -> None:
        state = m.SeenState.from_dict(
            {
                "ids": ["binance:https://example.com/old:old"],
                "seeded": ["binance", "bybit", "coinbase", "upbit"],
            }
        )
        self.assertEqual(
            state.to_dict()["seeded"],
            [
                "binance_delist",
                "binance_listing",
                "bybit",
                "coinbase_currencies",
                "coinbase_status",
                "upbit",
            ],
        )
        events = m.ingest_source(
            "binance_listing",
            [_item("binance", "Will List ZZZ (ZZZ)", "https://example.com/new")],
            state,
            False,
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["tickers"], ["ZZZ"])

    def test_run_once_creates_client_when_sources_omitted(self) -> None:
        got: dict = {}

        class Dummy:
            def close(self) -> None:
                got["closed"] = True

        def fake_make():
            got["made"] = True
            return Dummy()

        def fetch(client):
            got["client"] = client
            return []

        orig_sources = m.SOURCES
        orig_make = m.make_client
        m.SOURCES = (_source("bybit", "bybit", fetch),)
        m.make_client = fake_make
        self.addCleanup(lambda: setattr(m, "SOURCES", orig_sources))
        self.addCleanup(lambda: setattr(m, "make_client", orig_make))
        m.run_once({"ids": [], "seeded": ["bybit"]}, False)
        self.assertTrue(got.get("made"))
        self.assertTrue(got.get("closed"))
        self.assertIsInstance(got.get("client"), Dummy)


class IndependentSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["LISTING_NEWS_PAPER_CACHE"] = self.tmp.name
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: os.environ.pop("LISTING_NEWS_PAPER_CACHE", None))

    def test_as_completed_emits_before_slow_source(self) -> None:
        fast_item = _item("bybit", "upcoming listing of FOO (FOO)", "https://example.com/fast")
        slow_item = _item(
            "coinbase",
            "Coinbase listed (BAR)",
            "https://example.com/slow",
            "new_crypto",
            "currency_list_change",
            "coinbase_currencies",
        )

        def fast(_client):
            time.sleep(0.05)
            return [fast_item]

        def slow(_client):
            time.sleep(0.45)
            return [slow_item]

        sources = [
            _source("coinbase_currencies", "coinbase", slow),
            _source("bybit", "bybit", fast),
        ]
        seen = {
            "ids": [],
            "seeded": ["bybit", "coinbase_currencies"],
        }
        times = []

        def on_event(ev):
            times.append((time.monotonic(), ev["venue"], ev["tickers"], ev["action"]))

        t0 = time.monotonic()
        events = m.run_once(seen, emit_skip=False, sources=sources, on_event=on_event)
        first_ms = (times[0][0] - t0) * 1000.0
        self.assertLess(first_ms, 250.0)
        self.assertEqual(times[0][1], "bybit")
        self.assertEqual(times[0][2], ["FOO"])
        self.assertEqual({(e["venue"], e["action"], tuple(e["tickers"])) for e in events}, {
            ("bybit", "BUY", ("FOO",)),
            ("coinbase", "BUY", ("BAR",)),
        })
        self.assertGreater((times[-1][0] - t0) * 1000.0, 350.0)

    def test_partial_failure_keeps_successful_source(self) -> None:
        listing = [_item("binance", "Will List FOO (FOO)", "https://example.com/b1")]

        def ok(_client):
            return listing

        def boom(_client):
            raise RuntimeError("Binance binance_delist 429")

        sources = [
            _source("binance_listing", "binance", ok),
            _source("binance_delist", "binance", boom),
        ]
        seen = {"ids": [], "seeded": ["binance_listing", "binance_delist"]}
        events = m.run_once(seen, emit_skip=False, sources=sources)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["tickers"], ["FOO"])
        self.assertEqual(events[0]["source_id"], "binance_listing")
        self.assertTrue(m.SeenState.from_dict(seen).is_seeded("binance_delist"))

    def test_coinbase_status_survives_currencies_429(self) -> None:
        status = [
            _item(
                "coinbase",
                "CT-USD Markets Open",
                "https://example.com/m",
                "",
                "markets_open",
                "coinbase_status",
            )
        ]

        def ok(_client):
            return status

        def boom(_client):
            raise RuntimeError("Coinbase currencies 429")

        sources = [
            _source("coinbase_status", "coinbase", ok),
            _source("coinbase_currencies", "coinbase", boom),
        ]
        seen = {"ids": [], "seeded": ["coinbase_status", "coinbase_currencies"]}
        events = m.run_once(seen, emit_skip=False, sources=sources)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_kind"], "markets_open")
        self.assertEqual(events[0]["action"], "BUY")

    def test_duplicate_ids_in_one_batch_emit_once(self) -> None:
        row = _item("bybit", "upcoming listing of FOO (FOO)", "https://example.com/x")

        def fetch(_client):
            return [row, dict(row)]

        sources = [_source("bybit", "bybit", fetch)]
        seen = {"ids": [], "seeded": ["bybit"]}
        events = m.run_once(seen, emit_skip=False, sources=sources)
        self.assertEqual(len(events), 1)

    def test_late_source_does_not_catch_up_burst(self) -> None:
        calls = {"n": 0}

        def slow(_client):
            calls["n"] += 1
            time.sleep(0.12)
            return []

        sources = [_source("bybit", "bybit", slow)]
        seen = m.SeenState(seeded=["bybit"])
        t0 = time.monotonic()
        m.run_scheduler(
            loops=2,
            interval=0.04,
            emit_skip=False,
            sources=sources,
            seen=seen,
        )
        elapsed = time.monotonic() - t0
        self.assertEqual(calls["n"], 2)
        self.assertGreaterEqual(elapsed, 0.20)


class CycleMetricsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["LISTING_NEWS_PAPER_CACHE"] = self.tmp.name
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: os.environ.pop("LISTING_NEWS_PAPER_CACHE", None))

    def _cycles(self) -> list:
        path = Path(self.tmp.name, "cycles.jsonl")
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def test_parse_retry_after(self) -> None:
        self.assertEqual(m.parse_retry_after("120"), 120.0)
        self.assertEqual(m.parse_retry_after("0"), 0.0)
        self.assertIsNone(m.parse_retry_after(""))
        self.assertIsNone(m.parse_retry_after(None))
        self.assertIsNone(m.parse_retry_after("nope"))
        self.assertEqual(m.cap_retry_after(99999.0), m.RETRY_AFTER_CAP_S)
        from datetime import datetime, timedelta, timezone
        from email.utils import format_datetime

        now = 1_700_000_000.0
        dt = datetime.fromtimestamp(now, tz=timezone.utc) + timedelta(seconds=30)
        got = m.parse_retry_after(format_datetime(dt, usegmt=True), now=now)
        self.assertIsNotNone(got)
        self.assertAlmostEqual(got or 0.0, 30.0, delta=1.0)

    def test_schedule_next_due_uses_retry_after_and_backoff(self) -> None:
        started = 100.0
        received = 101.0
        due = m.schedule_next_due(started, received, 15.0, 0, None)
        self.assertEqual(due, 115.0)
        due_err = m.schedule_next_due(started, received, 15.0, 1, None)
        self.assertEqual(due_err, 130.0)
        due_ra = m.schedule_next_due(started, received, 15.0, 1, 60.0)
        self.assertEqual(due_ra, 161.0)
        due_cap = m.schedule_next_due(started, received, 15.0, 1, 99999.0)
        self.assertEqual(due_cap, received + m.RETRY_AFTER_CAP_S)

    def test_empty_poll_records_http_ms(self) -> None:
        def fetch(_client):
            return m.FetchResult(
                source_id="coinbase_currencies",
                items=[],
                status_code=200,
                http_ms=12.5,
                received_at=time.monotonic(),
                error=None,
                retry_after_s=None,
            )

        sources = [_source("coinbase_currencies", "coinbase", fetch)]
        seen = {"ids": [], "seeded": ["coinbase_currencies"]}
        events = m.run_once(seen, emit_skip=False, sources=sources)
        self.assertEqual(events, [])
        recs = self._cycles()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["kind"], "cycle")
        self.assertEqual(recs[0]["http_ms"], 12.5)
        self.assertEqual(recs[0]["status_code"], 200)
        self.assertEqual(recs[0]["fetched"], 0)
        self.assertEqual(recs[0]["error"], None)
        self.assertIn("cycle_elapsed_s", recs[0])
        self.assertIsNone(recs[0]["start_interval_s"])

    def test_start_interval_is_previous_start_gap(self) -> None:
        starts = []

        def fetch(_client):
            starts.append(time.monotonic())
            time.sleep(0.03)
            return m.FetchResult("bybit", [], 200, 5.0, time.monotonic(), None, None)

        sources = [_source("bybit", "bybit", fetch)]
        seen = m.SeenState(seeded=["bybit"])
        m.run_scheduler(
            loops=2,
            interval=0.08,
            emit_skip=False,
            sources=sources,
            seen=seen,
        )
        recs = self._cycles()
        self.assertEqual(len(recs), 2)
        self.assertIsNone(recs[0]["start_interval_s"])
        self.assertGreaterEqual(recs[1]["start_interval_s"], 0.07)
        self.assertAlmostEqual(recs[1]["start_interval_s"], starts[1] - starts[0], delta=0.03)

    def test_retry_after_delays_next_fetch(self) -> None:
        calls = {"n": 0}

        def boom(_client):
            calls["n"] += 1
            return m.FetchResult("bybit", [], 429, 1.0, time.monotonic(), "Bybit 429", 0.18)

        sources = [_source("bybit", "bybit", boom)]
        seen = m.SeenState(seeded=["bybit"])
        t0 = time.monotonic()
        m.run_scheduler(
            loops=2,
            interval=0.04,
            emit_skip=False,
            sources=sources,
            seen=seen,
        )
        elapsed = time.monotonic() - t0
        self.assertEqual(calls["n"], 2)
        self.assertGreaterEqual(elapsed, 0.18)
        recs = self._cycles()
        self.assertEqual(recs[0]["status_code"], 429)
        self.assertEqual(recs[0]["retry_after_s"], 0.18)
        self.assertGreaterEqual(recs[0]["consecutive_errors"], 1)

    def test_consecutive_errors_lengthen_wait(self) -> None:
        calls = {"n": 0}

        def boom(_client):
            calls["n"] += 1
            raise RuntimeError("Bybit down")

        sources = [_source("bybit", "bybit", boom)]
        seen = m.SeenState(seeded=["bybit"])
        t0 = time.monotonic()
        m.run_scheduler(
            loops=2,
            interval=0.05,
            emit_skip=False,
            sources=sources,
            seen=seen,
        )
        elapsed = time.monotonic() - t0
        self.assertEqual(calls["n"], 2)
        # first error: 2x interval from start before the second attempt
        self.assertGreaterEqual(elapsed, 0.10)
        recs = self._cycles()
        self.assertEqual([r["consecutive_errors"] for r in recs], [1, 2])

    def test_429_does_not_seed(self) -> None:
        def boom(_client):
            return m.FetchResult("bybit", [], 429, 3.0, time.monotonic(), "Bybit 429", 1.0)

        sources = [_source("bybit", "bybit", boom)]
        seen = {"ids": [], "seeded": []}
        events = m.run_once(seen, emit_skip=False, sources=sources)
        self.assertEqual(events, [])
        self.assertEqual(seen["seeded"], [])

    def test_judge_and_emit_ms_are_split(self) -> None:
        item = _item("bybit", "upcoming listing of FOO (FOO)", "https://example.com/x")
        item["t_http_start"] = time.monotonic() - 0.02
        item["t_http_end"] = time.monotonic() - 0.01
        ev = m.paper_event(item)
        self.assertIsNotNone(ev["timing"]["http_ms"])
        self.assertIsNotNone(ev["timing"]["judge_ms"])
        self.assertIsNone(ev["timing"]["emit_ms"])
        self.assertNotIn("process_ms", ev["timing"])

        seen = m.SeenState(seeded=["bybit"])
        emitted = []

        def on_event(_ev):
            time.sleep(0.05)
            emitted.append(_ev)

        events = m.ingest_source("bybit", [item], seen, False, on_event)
        self.assertEqual(len(events), 1)
        self.assertGreaterEqual(events[0]["timing"]["emit_ms"], 45.0)
        self.assertEqual(len(emitted), 1)

    def test_http_json_reads_retry_after(self) -> None:
        class Resp:
            status_code = 429
            text = "{}"
            headers = {"Retry-After": "42"}

        class Client:
            def get(self, url, timeout=None):
                return Resp()

        code, data, t0, t1, retry_after_s = m.http_json(Client(), "https://example.invalid/")
        self.assertEqual(code, 429)
        self.assertEqual(retry_after_s, 42.0)
        self.assertGreaterEqual(t1, t0)


if __name__ == "__main__":
    unittest.main()
