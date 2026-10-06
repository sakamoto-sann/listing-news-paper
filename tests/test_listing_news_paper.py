#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import listing_news_paper as m  # noqa: E402


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
            }
        )
        self.assertEqual(ev["orders"], 0)
        self.assertEqual(ev["mode"], "paper")
        self.assertEqual(ev["action"], "BUY")
        self.assertEqual(ev["tickers"], ["FOO"])


class SeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["LISTING_NEWS_PAPER_CACHE"] = self.tmp.name
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: os.environ.pop("LISTING_NEWS_PAPER_CACHE", None))

    def _item(self, venue: str, title: str, url: str, type_key: str = "new_crypto") -> dict:
        return {
            "venue": venue,
            "title": title,
            "url": url,
            "type_key": type_key,
            "publish_ms": 1,
        }

    def test_first_poll_seeds_and_emits_nothing(self) -> None:
        items = [
            self._item("binance", "Will List FOO (FOO)", "https://example.com/b1"),
            self._item("bybit", "upcoming listing of BAR (BAR)", "https://example.com/y1"),
            self._item("upbit", "돌핀(POD) 신규 거래지원 안내", "https://example.com/u1"),
            self._item("coinbase", "Coinbase listed (FOO)", "https://example.com/c1"),
        ]
        orig = m.fetch_all
        m.fetch_all = lambda: items  # type: ignore[assignment]
        self.addCleanup(lambda: setattr(m, "fetch_all", orig))
        seen = {"ids": [], "seeded": []}
        events = m.run_once(seen, emit_skip=True)
        self.assertEqual(events, [])
        self.assertEqual(seen["seeded"], ["binance", "bybit", "coinbase", "upbit"])
        self.assertEqual(len(seen["ids"]), 4)
        saved = json.loads(Path(self.tmp.name, "seen.json").read_text())
        self.assertEqual(saved["seeded"], ["binance", "bybit", "coinbase", "upbit"])

    def test_second_poll_emits_only_new(self) -> None:
        first = [self._item("binance", "Will List FOO (FOO)", "https://example.com/b1")]
        second = first + [
            self._item("binance", "Will List BAZ (BAZ)", "https://example.com/b2"),
        ]
        state = {"items": first}

        def fake_fetch() -> list:
            return list(state["items"])

        orig = m.fetch_all
        m.fetch_all = fake_fetch  # type: ignore[assignment]
        self.addCleanup(lambda: setattr(m, "fetch_all", orig))
        seen = {"ids": [], "seeded": []}
        self.assertEqual(m.run_once(seen, emit_skip=False), [])
        state["items"] = second
        events = m.run_once(seen, emit_skip=False)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["action"], "BUY")
        self.assertEqual(events[0]["tickers"], ["BAZ"])
        self.assertEqual(events[0]["orders"], 0)


if __name__ == "__main__":
    unittest.main()
