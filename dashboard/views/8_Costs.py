"""
Costs Page — API spend dashboard.

Headline answers four questions without having to scroll:

  1. How much have we spent this month? (MTD total)
  2. If we keep the current pace, where do we end the month? (projected burn)
  3. What does it cost us, on average, to generate one product? (cost / product)
  4. What does it cost us to find a winner? (cost / winner)

Layered below are the drilldowns — per-provider bar chart, per-endpoint
breakdown, the per-run list newest-first.

Data sources:
  - Google Sheet "API Costs" tab (Python side: Discover pipeline runs)
  - Node page-cloner JSONL log at $PAGE_CLONER_COST_LOG (or its default
    path under the sibling Movanella/page-cloner repo). Merged in-place.
    A cross-process sync into the same Sheet is still on the roadmap; until
    then we just read both and concatenate.

Time-window filter at the top of the page. MTD is the default because the
"projected monthly burn" metric only makes sense against month-to-date data.
"""

from __future__ import annotations

import os
import sys
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import json
import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Default JSONL path for the Node page-cloner's local cost log. The Node side
# doesn't write to the Google Sheet directly — it dumps JSONL that the worker
# will eventually slurp. Until that slurp is built we merge it in here so the
# dashboard shows the full picture.
DEFAULT_NODE_LOG = (
    Path(__file__).parent.parent.parent.parent
    / "Movanella" / "page-cloner" / "data" / "cost-log.jsonl"
)
NODE_LOG_PATH = Path(os.environ.get("PAGE_CLONER_COST_LOG", DEFAULT_NODE_LOG))

WINDOWS = ["Month-to-date", "Last 7 days", "Last 30 days", "All time"]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_sheet_records(store) -> list[dict]:
    """Pull all rows from the 'API Costs' Sheet tab. Returns [] on any error
    (sheet may not exist yet if no run has persisted since the CostTracker
    rollout) — the page degrades gracefully."""
    try:
        return store.get_cost_records(limit=10_000)
    except Exception:
        return []


def _load_node_jsonl(path: Path) -> list[dict]:
    """Tail the Node JSONL log — ignore malformed lines rather than crashing,
    since the log is append-only from a different process."""
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _normalise(record: dict) -> dict:
    """Coerce records from both sources into a single schema. Both sides
    already use the same field names — this just canonicalises types."""
    try:
        cost = float(record.get("cost_usd", 0) or 0)
    except (TypeError, ValueError):
        cost = 0.0
    estimated = record.get("estimated")
    if isinstance(estimated, str):
        estimated_bool = estimated.lower() in ("yes", "true", "1")
    else:
        estimated_bool = bool(estimated)
    return {
        "timestamp": str(record.get("timestamp") or ""),
        "run_id": str(record.get("run_id") or ""),
        "run_type": str(record.get("run_type") or ""),
        "provider": str(record.get("provider") or "unknown"),
        "endpoint": str(record.get("endpoint") or ""),
        "units": str(record.get("units") or ""),
        "cost_usd": cost,
        "context": str(record.get("context") or ""),
        "estimated": estimated_bool,
    }


# ---------------------------------------------------------------------------
# Window filtering
# ---------------------------------------------------------------------------

def _window_start(label: str, now: datetime) -> datetime | None:
    """Return the lower-bound timestamp for the selected window, or None for
    'All time'. Using naive UTC so it lines up with how records are written."""
    if label == "All time":
        return None
    if label == "Month-to-date":
        return datetime(now.year, now.month, 1)
    if label == "Last 7 days":
        return now - pd.Timedelta(days=7)
    if label == "Last 30 days":
        return now - pd.Timedelta(days=30)
    return None


def _days_elapsed_in_month(now: datetime) -> int:
    """How many whole days of the current month have elapsed — used for the
    projected-burn calculation. Day 1 at 00:00 counts as 1 to avoid div/0."""
    return max(1, now.day)


def _days_in_month(now: datetime) -> int:
    return monthrange(now.year, now.month)[1]


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def main():
    st.title("💸 Costs")
    st.caption(
        "API spend across Anthropic, DataForSEO, SerpAPI, and fal.ai. "
        "Pulled from the *API Costs* Sheet tab + the Node page-cloner's local JSONL."
    )

    # ---- Data store ------------------------------------------------------
    try:
        from src.sheets.manager import get_data_store
        from src.core.models import ProductStatus

        store = get_data_store()
    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    # ---- Load + normalise both sources ----------------------------------
    sheet_rows = _load_sheet_records(store)
    node_rows = _load_node_jsonl(NODE_LOG_PATH)
    all_rows = [_normalise(r) for r in (sheet_rows + node_rows)]

    if not all_rows:
        st.info(
            "No cost records yet. Run a Discover cycle on **Research**, or a "
            "clone job on **Clone page**, and come back here — spend will "
            "populate automatically."
        )
        st.caption(f"Looked in: Sheet `API Costs` tab, JSONL `{NODE_LOG_PATH}`")
        return

    df_all = pd.DataFrame(all_rows)
    df_all["timestamp_dt"] = pd.to_datetime(df_all["timestamp"], errors="coerce")

    # ---- Window picker ---------------------------------------------------
    now = datetime.utcnow()
    col_w, col_spacer = st.columns([2, 6])
    with col_w:
        window = st.selectbox("Window", WINDOWS, index=0)

    start = _window_start(window, now)
    df = df_all if start is None else df_all[df_all["timestamp_dt"] >= start]

    if df.empty:
        st.info(f"No cost records in window '{window}'.")
        return

    # ---- Compute headline metrics ---------------------------------------
    total = float(df["cost_usd"].sum())

    # Burn projection only makes sense for MTD
    if window == "Month-to-date":
        elapsed = _days_elapsed_in_month(now)
        days = _days_in_month(now)
        projected_monthly = total / elapsed * days
        burn_label = f"Projected {now.strftime('%b')} total"
        burn_val = f"${projected_monthly:.2f}"
        burn_help = (
            f"Straight-line projection: ${total:.2f} spent in "
            f"{elapsed} days → ${projected_monthly:.2f} across {days} days."
        )
    else:
        burn_val = "—"
        burn_label = "Projected monthly burn"
        burn_help = "Projection is shown in Month-to-date view only."

    # Products generated / winners — scoped to the same window using
    # created_at. Sheet writes that field as 'YYYY-MM-DD HH:MM:SS'.
    try:
        all_products = store.get_products()
    except Exception:
        all_products = []

    def _in_window(p) -> bool:
        if start is None:
            return True
        try:
            ts = pd.to_datetime(p.created_at, errors="coerce")
        except Exception:
            return False
        if pd.isna(ts):
            return False
        return ts.to_pydatetime() >= start

    window_products = [p for p in all_products if _in_window(p)]
    window_winners = [
        p for p in window_products
        if p.test_status in (ProductStatus.WINNER.value, ProductStatus.SCALING.value)
    ]

    n_prod = len(window_products)
    n_win = len(window_winners)
    cost_per_product = (total / n_prod) if n_prod else None
    cost_per_winner = (total / n_win) if n_win else None

    # ---- Row of four metrics --------------------------------------------
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total spend", f"${total:.2f}", help=f"Sum of all API calls within {window.lower()}.")
    m2.metric(burn_label, burn_val, help=burn_help)
    m3.metric(
        "Cost per product",
        f"${cost_per_product:.3f}" if cost_per_product is not None else "—",
        help=f"Total spend ÷ products created in window ({n_prod}).",
    )
    m4.metric(
        "Cost per winner",
        f"${cost_per_winner:.2f}" if cost_per_winner is not None else "— (no winners yet)",
        help=f"Total spend ÷ winners in window ({n_win}).",
    )

    st.divider()

    # ---- Per-provider bar chart -----------------------------------------
    st.subheader("Spend by provider")
    by_provider = (
        df.groupby("provider", as_index=False)
          .agg(cost_usd=("cost_usd", "sum"), calls=("cost_usd", "size"),
               any_estimated=("estimated", "any"))
          .sort_values("cost_usd", ascending=False)
    )

    col_chart, col_table = st.columns([3, 2])
    with col_chart:
        chart_df = by_provider.set_index("provider")[["cost_usd"]].rename(
            columns={"cost_usd": "USD"}
        )
        st.bar_chart(chart_df, height=260)

    with col_table:
        display = by_provider.copy()
        display["cost_usd"] = display["cost_usd"].map(lambda v: f"${v:.4f}")
        display["estimated"] = display["any_estimated"].map(lambda b: "~" if b else "")
        display = display[["provider", "calls", "cost_usd", "estimated"]]
        display.columns = ["Provider", "Calls", "Cost", "Est."]
        st.dataframe(display, hide_index=True, use_container_width=True)

    st.divider()

    # ---- Per-endpoint breakdown -----------------------------------------
    st.subheader("Breakdown by endpoint")
    by_ep = (
        df.groupby(["provider", "endpoint"], as_index=False)
          .agg(
              calls=("cost_usd", "size"),
              cost_usd=("cost_usd", "sum"),
              any_estimated=("estimated", "any"),
          )
          .sort_values("cost_usd", ascending=False)
    )
    by_ep_display = by_ep.copy()
    by_ep_display["cost_usd"] = by_ep_display["cost_usd"].map(lambda v: f"${v:.4f}")
    by_ep_display["estimated"] = by_ep_display["any_estimated"].map(lambda b: "~" if b else "")
    by_ep_display = by_ep_display[["provider", "endpoint", "calls", "cost_usd", "estimated"]]
    by_ep_display.columns = ["Provider", "Endpoint", "Calls", "Cost", "Est."]
    st.dataframe(by_ep_display, hide_index=True, use_container_width=True)

    # ---- Recent runs -----------------------------------------------------
    st.subheader("Recent runs")
    st.caption("💡 Click a row to see the per-model breakdown for that run.")
    by_run = (
        df.groupby(["run_id", "run_type"], as_index=False)
          .agg(
              started=("timestamp_dt", "min"),
              calls=("cost_usd", "size"),
              cost_usd=("cost_usd", "sum"),
          )
          .sort_values("started", ascending=False)
          .head(25)
    )
    by_run_display = by_run.copy()
    by_run_display["started"] = by_run_display["started"].dt.strftime("%Y-%m-%d %H:%M")
    by_run_display["cost_usd_fmt"] = by_run_display["cost_usd"].map(lambda v: f"${v:.4f}")
    # Keep run_id on the displayed frame in a known column order; we look
    # it back up by positional index when the user clicks a row.
    by_run_display_render = by_run_display[
        ["started", "run_type", "run_id", "calls", "cost_usd_fmt"]
    ].rename(columns={
        "started": "Started",
        "run_type": "Type",
        "run_id": "Run ID",
        "calls": "Calls",
        "cost_usd_fmt": "Cost",
    })

    # selection_mode="single-row" + on_select="rerun" is Streamlit's
    # standard way to get per-row interactivity. We reset the frame's
    # index first so the selection indices line up 0..N-1 with the
    # displayed rows (otherwise the iloc lookup below is off).
    by_run_display_render = by_run_display_render.reset_index(drop=True)
    run_id_by_position = by_run_display.reset_index(drop=True)["run_id"]

    event = st.dataframe(
        by_run_display_render,
        hide_index=True,
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun",
        key="cost_recent_runs_table",
    )

    selected_rows = []
    if event is not None:
        # The event object exposes .selection.rows (Streamlit ≥1.35).
        # Defensive: older versions / degraded render may return None.
        try:
            selected_rows = list(event.selection.rows)
        except AttributeError:
            selected_rows = []

    if selected_rows:
        sel_idx = selected_rows[0]
        selected_run_id = str(run_id_by_position.iloc[sel_idx])
        run_rows = df[df["run_id"] == selected_run_id]

        if run_rows.empty:
            st.info(f"No cost records found for run `{selected_run_id}`.")
        else:
            # Per-provider rollup for this run
            by_prov = (
                run_rows.groupby("provider", as_index=False)
                .agg(
                    calls=("cost_usd", "size"),
                    cost_usd=("cost_usd", "sum"),
                    any_estimated=("estimated", "any"),
                )
                .sort_values("cost_usd", ascending=False)
            )
            run_total = float(by_prov["cost_usd"].sum())
            # Per-endpoint rollup (useful when one provider hits multiple
            # endpoints — e.g. anthropic with sonnet + haiku, or dataforseo
            # with both volume and SERP endpoints).
            by_ep_run = (
                run_rows.groupby(["provider", "endpoint"], as_index=False)
                .agg(
                    calls=("cost_usd", "size"),
                    cost_usd=("cost_usd", "sum"),
                    any_estimated=("estimated", "any"),
                )
                .sort_values("cost_usd", ascending=False)
            )

            st.markdown(
                f"#### Breakdown for `{selected_run_id}` — "
                f"${run_total:.4f} across {int(by_prov['calls'].sum())} calls"
            )

            col_a, col_b = st.columns(2)

            with col_a:
                st.caption("**By provider**")
                prov_display = by_prov.copy()
                prov_display["share"] = (
                    prov_display["cost_usd"] / max(run_total, 1e-12) * 100
                ).map(lambda p: f"{p:.0f}%")
                prov_display["cost_usd"] = prov_display["cost_usd"].map(lambda v: f"${v:.4f}")
                prov_display["estimated"] = prov_display["any_estimated"].map(
                    lambda b: "~" if b else ""
                )
                prov_display = prov_display[
                    ["provider", "calls", "cost_usd", "share", "estimated"]
                ]
                prov_display.columns = ["Provider", "Calls", "Cost", "Share", "Est."]
                st.dataframe(prov_display, hide_index=True, use_container_width=True)

            with col_b:
                st.caption("**By endpoint**")
                ep_display = by_ep_run.copy()
                ep_display["cost_usd"] = ep_display["cost_usd"].map(lambda v: f"${v:.4f}")
                ep_display["estimated"] = ep_display["any_estimated"].map(
                    lambda b: "~" if b else ""
                )
                ep_display = ep_display[
                    ["provider", "endpoint", "calls", "cost_usd", "estimated"]
                ]
                ep_display.columns = ["Provider", "Endpoint", "Calls", "Cost", "Est."]
                st.dataframe(ep_display, hide_index=True, use_container_width=True)

            # Collapsed raw-calls view for the curious: full per-call log.
            with st.expander(f"🔬 See all {len(run_rows)} individual calls", expanded=False):
                raw = run_rows[
                    ["timestamp", "provider", "endpoint", "units", "cost_usd", "context", "estimated"]
                ].copy()
                raw = raw.sort_values("timestamp")
                raw["cost_usd"] = raw["cost_usd"].map(lambda v: f"${v:.6f}")
                raw["estimated"] = raw["estimated"].map(lambda b: "~" if b else "")
                raw.columns = ["Timestamp", "Provider", "Endpoint", "Units", "Cost", "Context", "Est."]
                st.dataframe(raw, hide_index=True, use_container_width=True)

    # ---- Footer / diagnostics -------------------------------------------
    with st.expander("Data sources", expanded=False):
        st.caption(
            f"Sheet rows loaded: **{len(sheet_rows)}**  •  "
            f"JSONL rows loaded: **{len(node_rows)}**  •  "
            f"Window: **{window}**  •  "
            f"Records after filter: **{len(df)}**"
        )
        st.caption(f"JSONL path: `{NODE_LOG_PATH}`")
        if not NODE_LOG_PATH.exists():
            st.caption("*(JSONL file does not exist yet — Node page-cloner runs haven't been captured.)*")


main()
