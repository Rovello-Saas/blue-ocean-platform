"""
Background runner for the Discover pipeline.

Why this exists
---------------
Streamlit reruns the page script on every interaction, and when the user
navigates to a different page the current script is torn down mid-execution.
If Discover is running synchronously inside a page render (as it did before),
clicking any other nav item means the blocking `run_full_pipeline` call stops
feeding back to the UI and — worse — the `st.status` context dies while the
pipeline thread keeps chugging, so the user has no visibility into the run.

The fix is to push the actual pipeline work into a Python thread owned by a
module-level registry, and have the dashboard render a view of the registry
state. Module-level state lives in the interpreter, not in the page script or
Streamlit's session state, so it survives as long as the Python process does.

What this is NOT
----------------
- Not a job queue. Runs aren't persisted to disk — if the Streamlit process
  restarts, everything is gone. For our use case (ad-hoc manual discovery
  kicked off from the dashboard) that's fine. For scheduled discovery there's
  a separate auto_discovery scheduler.
- Not multi-user. A single Python process hosts the registry; two users
  sharing a deployment will see each other's runs. That's the same trust model
  as the rest of the dashboard.
- Not process-isolated. The pipeline runs in the same interpreter as the
  dashboard; a segfault inside e.g. anthropic's HTTP client would take the
  dashboard down with it. Anthropic's SDK is pure Python so in practice
  this is fine, but worth noting.

Concurrency
-----------
A lock guards registry mutation — reads also take the lock so dashboard code
never sees a half-updated RunState. Individual RunState field writes from the
worker thread are not locked (they're single-attribute Python assignments,
which are atomic under the GIL) but the reader should treat any single
snapshot as eventually-consistent: the message might be slightly stale.

Threads are `daemon=True` so a process exit doesn't hang on an in-flight
Discover run. If that's a problem later we can add a graceful-shutdown hook;
for now "kill the dashboard, restart" is the expected recovery path.
"""
from __future__ import annotations

import logging
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RunState:
    """Snapshot of one Discover run's progress.

    Mutated by the worker thread, read by the dashboard render loop. All
    timestamps are monotonic `time.time()` so we can compute elapsed without
    timezone headaches.
    """
    run_id: str
    started_at: float
    country_codes: list[str]
    # Worker fills these as it progresses
    finished_at: Optional[float] = None
    status: str = "running"           # running | done | error
    current_country: Optional[str] = None
    progress_msg: str = "starting…"
    # Structured progress — set by the pipeline's progress callback. `total`
    # is 0 for batched stages (ideation, volume check, Claude QA, final
    # write); the renderer should only show the "current/total" counter
    # when total > 0. `current_stage` is the short human label
    # (e.g. "Analyzing competition").
    current_stage: str = ""
    current: int = 0
    total: int = 0
    all_stats: list[dict] = field(default_factory=list)
    # Aggregates — filled at completion so the dashboard can render the cost
    # banner without re-walking all_stats.
    total_added: int = 0
    total_cost_usd: float = 0.0
    error: Optional[str] = None

    @property
    def elapsed_seconds(self) -> float:
        end = self.finished_at if self.finished_at else time.time()
        return max(0.0, end - self.started_at)

    @property
    def is_active(self) -> bool:
        return self.status == "running"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
# Module-level singleton dict. Streamlit's `sys.modules` cache means this
# instance is reused across page navigations within a single Python process —
# which is exactly what we want.

_runs: dict[str, RunState] = {}
_lock = threading.Lock()


def start_discovery(store, config, country_codes: list[str]) -> str:
    """Spawn a background thread that runs the pipeline for the given
    countries. Returns the run_id immediately; caller stores it in session
    state and polls `get_run` on subsequent page renders.

    The worker imports `ResearchPipeline` lazily so this module can be
    imported from page-render code without dragging in the whole research
    stack on import.
    """
    run_id = uuid.uuid4().hex[:8]
    state = RunState(
        run_id=run_id,
        started_at=time.time(),
        country_codes=list(country_codes),
    )
    with _lock:
        _runs[run_id] = state

    def _worker() -> None:
        # Lazy import — avoids circular imports and keeps the dashboard's
        # startup path light (research.pipeline pulls in anthropic,
        # dataforseo, serpapi…).
        try:
            from src.research.pipeline import ResearchPipeline
        except Exception as e:
            state.status = "error"
            state.error = f"Import failed: {e}\n{traceback.format_exc()}"
            state.finished_at = time.time()
            logger.error("Discovery worker import failed: %s", e, exc_info=True)
            return

        try:
            pipeline = ResearchPipeline(store, config)
            for code in country_codes:
                state.current_country = code
                state.current_stage = "starting"
                state.current = 0
                state.total = 0
                state.progress_msg = f"{code}: starting…"
                lang = _country_language(config.countries, code)
                logger.info("[bg run %s] starting %s/%s", run_id, code, lang)

                def _on_progress(stage: str, current: int, total: int,
                                 _code: str = code) -> None:
                    """Copy pipeline progress into the shared RunState. Attr
                    writes are atomic under the GIL so the dashboard
                    render loop sees a consistent snapshot even without
                    taking the registry lock. `_code` is bound as a default
                    argument so the closure captures the *current* country
                    rather than late-binding the loop variable."""
                    state.current_stage = stage
                    state.current = current
                    state.total = total
                    if total > 0:
                        state.progress_msg = f"{_code}: {stage} ({current}/{total})"
                    else:
                        state.progress_msg = f"{_code}: {stage}…"

                stats = pipeline.run_full_pipeline(
                    country=code,
                    language=lang,
                    progress_cb=_on_progress,
                )
                state.all_stats.append(stats)
                added = int(stats.get("products_added_to_sourcing", 0) or 0)
                logger.info("[bg run %s] %s done — %d added", run_id, code, added)

            state.total_added = sum(
                int(s.get("products_added_to_sourcing", 0) or 0)
                for s in state.all_stats
            )
            state.total_cost_usd = sum(
                float(s.get("cost_total_usd", 0) or 0) for s in state.all_stats
            )
            state.progress_msg = (
                f"Done — {state.total_added} added across "
                f"{len(country_codes)} countr{'y' if len(country_codes) == 1 else 'ies'}"
            )
            state.status = "done"
        except Exception as e:
            state.status = "error"
            state.error = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            state.progress_msg = f"Error: {type(e).__name__}: {e}"
            logger.error("[bg run %s] failed: %s", run_id, e, exc_info=True)
        finally:
            state.finished_at = time.time()

    t = threading.Thread(
        target=_worker,
        name=f"discovery-{run_id}",
        daemon=True,
    )
    t.start()
    return run_id


def _country_language(countries_list, code: str) -> str:
    """Mirror of the helper in 2_Research.py — we keep a local copy so this
    module has no dashboard-view dependencies."""
    for c in countries_list:
        if isinstance(c, dict) and c.get("code") == code:
            return c.get("language", "de")
    return "de"


def get_run(run_id: str) -> Optional[RunState]:
    with _lock:
        return _runs.get(run_id)


def get_active_runs() -> list[RunState]:
    """All runs with status == 'running'. Typically 0 or 1 in practice, but
    the API supports multi so the scheduler could enqueue several."""
    with _lock:
        return [r for r in _runs.values() if r.is_active]


def get_recent_runs(limit: int = 10) -> list[RunState]:
    """Newest-first list of runs (any status). Used by any future 'history'
    view; the main dashboard only needs the active ones."""
    with _lock:
        return sorted(_runs.values(), key=lambda r: r.started_at, reverse=True)[:limit]


def clear_finished(older_than_seconds: float = 3600) -> int:
    """Drop finished runs from the registry after they've aged out, so the
    dict doesn't grow forever in a long-lived dashboard process. Called
    opportunistically from the Research view."""
    now = time.time()
    dropped = 0
    with _lock:
        for rid in list(_runs.keys()):
            r = _runs[rid]
            if r.status in ("done", "error") and r.finished_at:
                if (now - r.finished_at) > older_than_seconds:
                    del _runs[rid]
                    dropped += 1
    return dropped
