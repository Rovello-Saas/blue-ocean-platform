"""
Google Trends validation for Best Sellers candidates.

Purpose: before we spend paid-API cost (DataForSEO, SerpAPI, AliExpress) on a
candidate from the Best Sellers report, do a cheap free-tier Trends check:

  - Is the term still rising, or is it already peaked / declining?
  - Is it seasonal and we're before/after peak?
  - Is there any search history at all, or is it a fad-only spike?

Three-way output per candidate:
  "rising"     — clearly trending up (buy signal)
  "stable"     — flat with real volume (ok to proceed)
  "declining"  — trending down (skip)
  "unknown"    — Trends returned nothing / errored (fail open, proceed)

Scaling: pytrends accepts up to 5 keywords per `build_payload` call. For
100-300 candidates per weekly run that's 20-60 API calls, well within free
tier limits even with aggressive backoff on 429. If we ever scale past that,
the backend can be swapped for SerpApi's Trends wrapper or the official
Google Trends API (alpha 2025-07) without changing this interface.

Rate-limit reality (2024-2026): free pytrends is flaky. We (a) space calls
generously, (b) catch 429 and back off exponentially, (c) fail open — an
unreachable Trends API never kills a research run, it just skips the
validation layer.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd

try:
    from pytrends.request import TrendReq
    _PYTRENDS_AVAILABLE = True
except ImportError:  # pragma: no cover — defensive
    _PYTRENDS_AVAILABLE = False
    TrendReq = None  # type: ignore

logger = logging.getLogger(__name__)


# Timeframe: 90 days gives enough signal to distinguish "rising" from
# "seasonal blip" without falling into noise from 7-day windows.
DEFAULT_TIMEFRAME = "today 3-m"

# Geo mapping: pytrends uses the ISO-alpha-2 code (or "" for worldwide).
# Our pipeline's country is already in that shape.

# Movement thresholds — simple linear fit on the 90-day interest curve.
# The numbers are conservative; we'd rather let a mediocre candidate through
# than kill a genuine winner because the curve was a bit noisy.
RISING_SLOPE_THRESHOLD = 0.15    # ≥ 0.15 interest-points-per-day = rising
DECLINING_SLOPE_THRESHOLD = -0.15  # ≤ -0.15 = declining
NOISE_FLOOR_AVG = 5.0            # if mean interest < 5, treat as no-data


@dataclass
class TrendResult:
    keyword: str
    direction: str           # "rising" / "stable" / "declining" / "unknown"
    slope: float = 0.0       # linear slope of the 90-day interest curve
    avg_interest: float = 0.0
    recent_avg: float = 0.0  # last 2 weeks mean
    older_avg: float = 0.0   # first 2 weeks mean
    reason: str = ""


# ---------------------------------------------------------------------------

class TrendsValidator:
    """Batched, backoff-retrying Google Trends validator.

    Usage:
        v = TrendsValidator(geo="DE")
        results = v.validate_batch(["wooden watch", "phone holder motorcycle"])
        # {"wooden watch": TrendResult(direction="rising", ...), ...}
    """

    def __init__(
        self,
        geo: str = "DE",
        hl: str = "en-US",           # interface language; doesn't affect results
        timeframe: str = DEFAULT_TIMEFRAME,
        *,
        batch_size: int = 5,          # pytrends hard cap
        inter_batch_delay: float = 2.0,  # seconds between successful calls
        max_retries: int = 3,
    ):
        if not _PYTRENDS_AVAILABLE:
            raise RuntimeError(
                "pytrends is not installed. `pip install pytrends>=4.9.2`"
            )
        self.geo = geo.upper() if geo else ""
        self.hl = hl
        self.timeframe = timeframe
        self.batch_size = max(1, min(5, batch_size))
        self.inter_batch_delay = max(0.0, inter_batch_delay)
        self.max_retries = max(1, max_retries)
        # pytrends constructor is cheap; reuse one instance across batches
        # (its internal session caches cookies which is what we want).
        self._client: Optional[TrendReq] = None

    def _get_client(self) -> TrendReq:
        if self._client is None:
            # retries/backoff handled by us (pytrends' built-in is weaker).
            self._client = TrendReq(hl=self.hl, tz=0, timeout=(10, 25))
        return self._client

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def validate_batch(self, keywords: list[str]) -> dict[str, TrendResult]:
        """Validate a list of search terms. Returns {keyword: TrendResult}.

        Batches into groups of `batch_size` (max 5), spaces calls with
        `inter_batch_delay`, and applies exponential backoff on 429/network
        errors up to `max_retries`. Failures fall through as direction
        "unknown" rather than raising — callers should treat "unknown" as
        "not blocked."
        """
        if not keywords:
            return {}

        # Dedupe while preserving order (Trends results are case-insensitive).
        seen: set[str] = set()
        ordered: list[str] = []
        for k in keywords:
            lk = (k or "").strip().lower()
            if lk and lk not in seen:
                seen.add(lk)
                ordered.append(k.strip())

        results: dict[str, TrendResult] = {}
        for i in range(0, len(ordered), self.batch_size):
            batch = ordered[i:i + self.batch_size]
            batch_results = self._try_batch_with_retry(batch)
            results.update(batch_results)
            # Only pause between SUCCESSFUL batches — retries have their own
            # backoff. Don't sleep after the final batch.
            if i + self.batch_size < len(ordered):
                time.sleep(self.inter_batch_delay)
        return results

    # -----------------------------------------------------------------------
    # Internals
    # -----------------------------------------------------------------------

    def _try_batch_with_retry(self, batch: list[str]) -> dict[str, TrendResult]:
        """One batch of up to 5 keywords, with exponential backoff."""
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._run_batch(batch)
            except Exception as e:
                wait = min(60, 2 ** attempt * 5)
                msg = str(e).lower()
                if "429" in msg or "too many" in msg or "rate" in msg:
                    logger.warning(
                        "Trends batch hit rate limit (attempt %d/%d) — "
                        "sleeping %ds: %s",
                        attempt, self.max_retries, wait, e,
                    )
                else:
                    logger.warning(
                        "Trends batch failed (attempt %d/%d: %s) — sleeping %ds",
                        attempt, self.max_retries, type(e).__name__, wait,
                    )
                if attempt < self.max_retries:
                    time.sleep(wait)
        # Retries exhausted — fail open.
        logger.warning(
            "Trends validation failed after %d retries for batch %s — "
            "marking as 'unknown' (fail open)",
            self.max_retries, batch,
        )
        return {kw: TrendResult(kw, "unknown", reason="api_unreachable")
                for kw in batch}

    def _run_batch(self, batch: list[str]) -> dict[str, TrendResult]:
        """Single Trends call. Raises on any error for the retry wrapper."""
        client = self._get_client()
        client.build_payload(
            kw_list=batch,
            cat=0,
            timeframe=self.timeframe,
            geo=self.geo,
            gprop="",
        )
        df = client.interest_over_time()
        if df is None or df.empty:
            return {kw: TrendResult(kw, "unknown", reason="no_data") for kw in batch}

        # Drop the "isPartial" column pytrends adds.
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        out: dict[str, TrendResult] = {}
        for kw in batch:
            # pytrends sometimes cases or trims differently; match column by
            # case-insensitive equality.
            col = next(
                (c for c in df.columns if c.lower().strip() == kw.lower().strip()),
                None,
            )
            if col is None:
                out[kw] = TrendResult(kw, "unknown", reason="missing_column")
                continue
            series = df[col].fillna(0).astype(float)
            out[kw] = self._classify(kw, series)
        return out

    @staticmethod
    def _classify(keyword: str, series: "pd.Series") -> TrendResult:
        """Classify a 90-day interest series as rising / stable / declining.

        Uses two signals:
          1. Linear slope via numpy polyfit (direction + magnitude)
          2. Recent vs older average (last 2 wks vs first 2 wks)

        The avg-comparison catches late-inflection curves the linear fit
        misses. We require BOTH to agree for a "rising" or "declining"
        verdict; anything mixed falls through as "stable".
        """
        if series.empty:
            return TrendResult(keyword, "unknown", reason="empty_series")

        avg = float(series.mean())
        if avg < NOISE_FLOOR_AVG:
            return TrendResult(
                keyword, "unknown", avg_interest=avg,
                reason=f"below_noise_floor ({avg:.1f} < {NOISE_FLOOR_AVG})",
            )

        # Linear slope — use numeric x = 0..N-1
        try:
            import numpy as np
            xs = np.arange(len(series))
            slope, _ = np.polyfit(xs, series.values, 1)
            slope = float(slope)
        except Exception:
            slope = 0.0

        # Recent vs older (robust against noisy mid-curve)
        n = max(1, len(series) // 6)  # ~2 weeks of 90 days
        older = float(series.iloc[:n].mean()) if len(series) >= 2 * n else avg
        recent = float(series.iloc[-n:].mean()) if len(series) >= 2 * n else avg

        rising_fit = slope >= RISING_SLOPE_THRESHOLD
        declining_fit = slope <= DECLINING_SLOPE_THRESHOLD
        rising_avg = recent > older * 1.15    # 15%+ lift
        declining_avg = recent < older * 0.85  # 15%+ drop

        if rising_fit and rising_avg:
            direction, reason = "rising", f"slope={slope:.2f}, recent/older={recent/older:.2f}"
        elif declining_fit and declining_avg:
            direction, reason = "declining", f"slope={slope:.2f}, recent/older={recent/older:.2f}"
        else:
            direction, reason = "stable", f"slope={slope:.2f}, recent/older={recent/(older or 1):.2f}"

        return TrendResult(
            keyword=keyword,
            direction=direction,
            slope=round(slope, 3),
            avg_interest=round(avg, 1),
            recent_avg=round(recent, 1),
            older_avg=round(older, 1),
            reason=reason,
        )


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

def validate_terms(
    terms: list[str],
    geo: str = "DE",
    *,
    timeframe: str = DEFAULT_TIMEFRAME,
    batch_size: int = 5,
    inter_batch_delay: float = 2.0,
    max_retries: int = 3,
) -> dict[str, TrendResult]:
    """One-call convenience for the pipeline.

    Returns a dict mapping each input term to a TrendResult. Missing terms
    (e.g. Trends returned no data) are still included with direction
    "unknown" — the caller decides whether to keep or drop them.

    Fails open at the module level: if pytrends isn't installed or the
    validator can't initialise, returns an empty dict rather than raising.
    """
    if not terms:
        return {}
    if not _PYTRENDS_AVAILABLE:
        logger.warning("pytrends not installed — skipping Trends validation")
        return {}
    try:
        v = TrendsValidator(
            geo=geo,
            timeframe=timeframe,
            batch_size=batch_size,
            inter_batch_delay=inter_batch_delay,
            max_retries=max_retries,
        )
        return v.validate_batch(terms)
    except Exception as e:
        logger.warning("Trends validator init failed (%s: %s) — skipping",
                       type(e).__name__, e)
        return {}
