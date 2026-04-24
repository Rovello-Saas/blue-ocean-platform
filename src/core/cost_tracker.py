"""
Cost tracking for all paid APIs used by the platform.

Usage pattern:
    tracker = CostTracker(run_id="disc_20260420_1", run_type="discover")

    # Pass tracker to each API-using module. Each call records one entry.
    kws = llm_ideation.generate_keywords(..., cost_tracker=tracker)
    vol = keyword_planner.validate_keywords(..., cost_tracker=tracker)
    comp = competition.analyze_competition(..., cost_tracker=tracker)

    # At end of run — show to user, persist to sheet
    print(tracker.summary())
    tracker.persist(store)

Design principles:
- Tracker is passed explicitly (no globals / thread-local magic) so call
  paths are auditable and tests can assert on cost records directly.
- Pricing constants live here in one place. Update when providers change.
- Exact costs preferred (token counts from LLM responses, `money.total` from
  DataForSEO). Estimates used only when provider doesn't return cost data.
- Batched persistence — one sheet write per run, not per API call.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Spill-to-disk directory for cost records that couldn't reach the sheet.
#
# Rationale: before this, a single transient failure (429 after retries,
# network glitch, revoked creds, mangled GOOGLE_SHEETS_SPREADSHEET_ID)
# caused `CostTracker.persist` to log + return, dropping the records. The
# API calls had already been billed by Anthropic / DataForSEO / SerpApi
# but we had no record of them. Cost reports silently under-reported.
#
# Now: on write failure we serialize records to JSON in a pending dir.
# On the NEXT successful persist call we drain the backlog first, then
# write the new batch. File gets unlink()ed only after a successful
# upload so repeated drain failures don't lose data.
#
# On Streamlit Cloud /tmp is wiped on reboot, which is acceptable — most
# failures are transient and the next discover run within the same
# session will drain them. For the catastrophic-config case (sheet ID
# cleared) the reboot that fixes the config also wipes pending records,
# but we still have Anthropic's own billing dashboard as ground truth.
# -----------------------------------------------------------------------------
_PENDING_DIR = Path(
    os.environ.get("BO_PENDING_COSTS_DIR")
    or (Path(tempfile.gettempdir()) / "bo_pending_costs")
)


# -----------------------------------------------------------------------------
# Pricing constants (2026, USD)
# Update these when providers change rates.
# -----------------------------------------------------------------------------

# Anthropic Claude Sonnet 4.5 — https://www.anthropic.com/pricing
CLAUDE_SONNET_INPUT_PER_MTOK = 3.00   # $3.00 / 1M input tokens
CLAUDE_SONNET_OUTPUT_PER_MTOK = 15.00  # $15.00 / 1M output tokens

# OpenAI GPT-4o — https://openai.com/api/pricing (kept for legacy fallback)
GPT4O_INPUT_PER_MTOK = 2.50
GPT4O_OUTPUT_PER_MTOK = 10.00

# DataForSEO Google Ads Search Volume (Live) — $0.05 per task of up to 1000
# kws. The response includes an exact `cost` field; we use that. This constant
# is only used when the response doesn't include cost for some reason.
DATAFORSEO_SEARCH_VOLUME_FALLBACK_PER_TASK = 0.05

# SerpAPI google_shopping — $0.015 per search on the Production plan
# ($75/mo / 5000 searches). User can override via config if on a different
# tier. No cost info in the response, so we always estimate.
SERPAPI_PER_CALL_ESTIMATE = 0.015

# fal.ai nano-banana v2 image edit/generation — $0.04 per image
# https://fal.ai/models/fal-ai/nano-banana-2
FAL_NANOBANANA_PER_IMAGE = 0.04


# Provider and endpoint identifiers used in log rows + breakdown keys.
# Keep these stable — downstream reports group by (provider, endpoint).
PROVIDER_ANTHROPIC = "anthropic"
PROVIDER_OPENAI = "openai"
PROVIDER_DATAFORSEO = "dataforseo"
PROVIDER_SERPAPI = "serpapi"
PROVIDER_FAL = "fal"


@dataclass
class CostRecord:
    """One API call's cost entry. Persisted as a row in API Costs sheet."""

    timestamp: str
    run_id: str
    run_type: str                 # "discover" | "page_clone" | "manual" | ...
    provider: str                 # PROVIDER_* constant
    endpoint: str                 # "claude-sonnet-4-5" | "keywords_data/search_volume/live" | ...
    units: str                    # human-readable units count, e.g. "3.2k in + 15.4k out tokens" or "150 kws"
    cost_usd: float               # exact or estimated in USD
    context: str = ""             # country code, keyword, product_id, whatever's useful
    estimated: bool = False       # True if cost_usd is estimated (not pulled from provider)


class CostTracker:
    """Per-run cost tracker. One instance lives for the duration of a Discover
    or Page Clone run, collects records, then gets persisted at the end.

    Thread-safety: not safe for concurrent use. Each run gets its own instance.
    If we later parallelise country runs within a single Discover, each needs
    its own tracker and we merge them with `.extend(other)` before persisting.
    """

    def __init__(
        self,
        run_id: Optional[str] = None,
        run_type: str = "unknown",
    ) -> None:
        self.run_id = run_id or f"{run_type}_{uuid.uuid4().hex[:8]}"
        self.run_type = run_type
        self.records: list[CostRecord] = []
        self.started_at = datetime.utcnow()

    # -------------------------------------------------------------------------
    # Recording API calls — one method per provider, all wrap _add().
    # -------------------------------------------------------------------------

    def record_anthropic(
        self,
        *,
        model: str,
        input_tokens: int,
        output_tokens: int,
        context: str = "",
    ) -> None:
        """Record an Anthropic API call. Exact cost from token counts."""
        if "sonnet" in model.lower() or "claude-sonnet" in model.lower():
            cost = (
                input_tokens * CLAUDE_SONNET_INPUT_PER_MTOK / 1_000_000
                + output_tokens * CLAUDE_SONNET_OUTPUT_PER_MTOK / 1_000_000
            )
        else:
            # Conservative fallback — use Sonnet rates, log a warning.
            logger.warning(
                "Unknown Anthropic model %r — using Sonnet pricing as fallback",
                model,
            )
            cost = (
                input_tokens * CLAUDE_SONNET_INPUT_PER_MTOK / 1_000_000
                + output_tokens * CLAUDE_SONNET_OUTPUT_PER_MTOK / 1_000_000
            )
        self._add(
            provider=PROVIDER_ANTHROPIC,
            endpoint=model,
            units=f"{_fmt_k(input_tokens)} in + {_fmt_k(output_tokens)} out tokens",
            cost_usd=cost,
            context=context,
            estimated=False,
        )

    def record_openai(
        self,
        *,
        model: str,
        input_tokens: int,
        output_tokens: int,
        context: str = "",
    ) -> None:
        """Record an OpenAI API call. Exact cost from token counts."""
        if "gpt-4o" in model.lower():
            cost = (
                input_tokens * GPT4O_INPUT_PER_MTOK / 1_000_000
                + output_tokens * GPT4O_OUTPUT_PER_MTOK / 1_000_000
            )
        else:
            logger.warning("Unknown OpenAI model %r — using GPT-4o pricing", model)
            cost = (
                input_tokens * GPT4O_INPUT_PER_MTOK / 1_000_000
                + output_tokens * GPT4O_OUTPUT_PER_MTOK / 1_000_000
            )
        self._add(
            provider=PROVIDER_OPENAI,
            endpoint=model,
            units=f"{_fmt_k(input_tokens)} in + {_fmt_k(output_tokens)} out tokens",
            cost_usd=cost,
            context=context,
            estimated=False,
        )

    def record_dataforseo(
        self,
        *,
        endpoint: str,
        cost_from_response: Optional[float],
        num_keywords: int,
        context: str = "",
    ) -> None:
        """Record a DataForSEO call. `cost_from_response` comes from the
        API's `cost` field; if None, falls back to the per-task estimate.
        """
        estimated = cost_from_response is None
        cost = (
            cost_from_response
            if cost_from_response is not None
            else DATAFORSEO_SEARCH_VOLUME_FALLBACK_PER_TASK
        )
        self._add(
            provider=PROVIDER_DATAFORSEO,
            endpoint=endpoint,
            units=f"{num_keywords} kws",
            cost_usd=cost,
            context=context,
            estimated=estimated,
        )

    def record_serpapi(
        self,
        *,
        engine: str,
        context: str = "",
        per_call_usd: Optional[float] = None,
    ) -> None:
        """Record a SerpAPI call. Always estimated — SerpAPI doesn't
        return cost info in-response. Override `per_call_usd` if on a
        different plan tier.
        """
        cost = per_call_usd if per_call_usd is not None else SERPAPI_PER_CALL_ESTIMATE
        self._add(
            provider=PROVIDER_SERPAPI,
            endpoint=engine,
            units="1 search",
            cost_usd=cost,
            context=context,
            estimated=True,
        )

    def record_fal(
        self,
        *,
        model: str,
        num_images: int = 1,
        context: str = "",
        per_image_usd: Optional[float] = None,
    ) -> None:
        """Record a fal.ai call. Uses the published per-image price unless
        overridden.
        """
        unit_cost = (
            per_image_usd if per_image_usd is not None else FAL_NANOBANANA_PER_IMAGE
        )
        cost = unit_cost * num_images
        self._add(
            provider=PROVIDER_FAL,
            endpoint=model,
            units=f"{num_images} image{'s' if num_images != 1 else ''}",
            cost_usd=cost,
            context=context,
            estimated=True,
        )

    def _add(
        self,
        *,
        provider: str,
        endpoint: str,
        units: str,
        cost_usd: float,
        context: str,
        estimated: bool,
    ) -> None:
        self.records.append(
            CostRecord(
                timestamp=datetime.utcnow().isoformat(timespec="seconds"),
                run_id=self.run_id,
                run_type=self.run_type,
                provider=provider,
                endpoint=endpoint,
                units=units,
                cost_usd=round(cost_usd, 6),
                context=context,
                estimated=estimated,
            )
        )

    # -------------------------------------------------------------------------
    # Reporting
    # -------------------------------------------------------------------------

    def total_usd(self) -> float:
        return round(sum(r.cost_usd for r in self.records), 4)

    def breakdown(self) -> list[dict]:
        """Group by (provider, endpoint). Returns a list of dicts sorted by cost desc."""
        agg: dict[tuple[str, str], dict] = {}
        for r in self.records:
            key = (r.provider, r.endpoint)
            if key not in agg:
                agg[key] = {
                    "provider": r.provider,
                    "endpoint": r.endpoint,
                    "calls": 0,
                    "cost_usd": 0.0,
                    "any_estimated": False,
                }
            agg[key]["calls"] += 1
            agg[key]["cost_usd"] += r.cost_usd
            agg[key]["any_estimated"] = agg[key]["any_estimated"] or r.estimated
        rows = list(agg.values())
        for row in rows:
            row["cost_usd"] = round(row["cost_usd"], 4)
        rows.sort(key=lambda r: r["cost_usd"], reverse=True)
        return rows

    def summary(self) -> str:
        """Human-readable one-liner + indented breakdown."""
        total = self.total_usd()
        lines = [f"Run {self.run_id} ({self.run_type}) — ${total:.4f} total"]
        for row in self.breakdown():
            flag = " ~" if row["any_estimated"] else ""
            lines.append(
                f"  {row['provider']:12s} {row['endpoint']:40s} "
                f"{row['calls']:>4d} calls   ${row['cost_usd']:>8.4f}{flag}"
            )
        return "\n".join(lines)

    def extend(self, other: "CostTracker") -> None:
        """Merge another tracker's records into this one. Used when a Discover
        run spawns per-country sub-trackers and we want a single persisted log.
        """
        self.records.extend(other.records)

    # -------------------------------------------------------------------------
    # Persistence — sheet write is one batch call at end of run.
    # -------------------------------------------------------------------------

    def persist(self, store) -> None:
        """Batch-write all records to the API Costs sheet.

        `store` is a `GoogleSheetsStore` instance (or anything implementing
        `append_cost_records`). We pass records out as dicts so the store
        layer owns the sheet schema.

        Failure handling:
          1. Drain any pending spill files from previous failed runs FIRST,
             so a healthy call retroactively captures the backlog.
          2. Write the current run's records.
          3. If either step fails, spill to disk and log loudly. Next
             successful call will drain the backlog.

        This method never raises — cost logging is observability, and a
        sheet outage shouldn't crash the pipeline. But it also never
        silently drops data anymore; every byte either lands in the sheet
        or lands in a pending file that a later call will pick up.
        """
        # Drain pending backlog first. If this fails we continue — we'd
        # rather write today's data than abort everything because an old
        # spill file is corrupt.
        self._drain_pending(store)

        if not self.records:
            return

        dict_rows = [
            {
                "timestamp": r.timestamp,
                "run_id": r.run_id,
                "run_type": r.run_type,
                "provider": r.provider,
                "endpoint": r.endpoint,
                "units": r.units,
                "cost_usd": r.cost_usd,
                "context": r.context,
                "estimated": "yes" if r.estimated else "no",
            }
            for r in self.records
        ]
        try:
            store.append_cost_records(dict_rows)
            logger.info(
                "Persisted %d cost records for run %s (total $%.4f)",
                len(dict_rows), self.run_id, self.total_usd(),
            )
        except Exception as e:
            # Sheet write failed. Spill to disk so the next run can
            # retry; log with exc_info so the Logs view surfaces it.
            spill_path = self._spill_to_disk(dict_rows, reason=str(e))
            logger.error(
                "Failed to persist %d cost records for run %s to sheet: %s. "
                "Spilled to %s — will retry on next persist.",
                len(dict_rows), self.run_id, e, spill_path,
                exc_info=True,
            )

    # -------------------------------------------------------------------------
    # Spill-to-disk helpers
    # -------------------------------------------------------------------------

    def _spill_to_disk(self, dict_rows: list[dict], reason: str) -> Path:
        """Serialize `dict_rows` to a pending JSON file. Returns the path
        written so callers can reference it in logs.

        One file per failed persist call. Filename includes run_id + unix
        timestamp so sorting drains oldest-first.
        """
        try:
            _PENDING_DIR.mkdir(parents=True, exist_ok=True)
            fname = f"{self.run_id}_{int(time.time() * 1000)}.json"
            path = _PENDING_DIR / fname
            path.write_text(json.dumps({
                "run_id": self.run_id,
                "spilled_at": datetime.now().isoformat(timespec="seconds"),
                "reason": reason,
                "records": dict_rows,
            }))
            return path
        except Exception as e:
            # Disk spill failed too (read-only FS? out of space?). At
            # this point we've truly lost the data — but at least log
            # with enough detail for forensic reconstruction from the
            # provider's own billing dashboard.
            logger.critical(
                "Cost record spill-to-disk ALSO failed for run %s: %s. "
                "DATA LOST — reconstruct from provider billing dashboards. "
                "Records: %s",
                self.run_id, e, dict_rows,
            )
            return Path("/dev/null")

    @staticmethod
    def _drain_pending(store) -> int:
        """Upload every pending spill file, oldest-first. Unlink each only
        after a successful sheet write. Returns the number of records
        successfully drained.

        Stops on the first upload failure so we don't hammer the sheet
        API if it's down — the remaining files stay on disk for the
        next persist call.
        """
        if not _PENDING_DIR.exists():
            return 0

        drained_records = 0
        drained_files = 0
        for path in sorted(_PENDING_DIR.glob("*.json")):
            try:
                payload = json.loads(path.read_text())
            except Exception as e:
                logger.warning(
                    "Skipping unreadable pending cost file %s: %s", path.name, e,
                )
                # Move corrupt file aside so it doesn't keep failing the
                # drain forever.
                try:
                    path.rename(path.with_suffix(".corrupt"))
                except Exception:
                    pass
                continue

            records = payload.get("records") or []
            if not records:
                path.unlink(missing_ok=True)
                continue

            try:
                store.append_cost_records(records)
            except Exception as e:
                logger.warning(
                    "Drain failed at %s (%d records): %s. "
                    "Leaving %d files on disk for next attempt.",
                    path.name, len(records), e,
                    len(list(_PENDING_DIR.glob("*.json"))),
                )
                break

            # Only unlink after confirmed upload — guarantees no data
            # loss if the next step throws.
            path.unlink(missing_ok=True)
            drained_records += len(records)
            drained_files += 1

        if drained_files:
            logger.info(
                "Drained %d pending cost file(s), %d records total",
                drained_files, drained_records,
            )
        return drained_records

    @staticmethod
    def pending_cost_records_count() -> int:
        """Total records sitting in spill files, for dashboard display."""
        if not _PENDING_DIR.exists():
            return 0
        total = 0
        for path in _PENDING_DIR.glob("*.json"):
            try:
                payload = json.loads(path.read_text())
                total += len(payload.get("records") or [])
            except Exception:
                pass
        return total


def _fmt_k(n: int) -> str:
    """Format a token count compactly: 3200 -> '3.2k', 150 -> '150'."""
    if n >= 1000:
        return f"{n / 1000:.1f}k"
    return str(n)
