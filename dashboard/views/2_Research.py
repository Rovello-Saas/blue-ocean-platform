"""
Research page — keyword inbox.

This page is intentionally NOT a CRUD screen for the Keywords tab. It's a
*decision queue*: every row is a product idea that's waiting for a human
yes/no, and the page exists to make that yes/no fast.

Layout (top → bottom):

  1. Header + the two ways to add to the inbox
        [Discover more]  [Add manually]
  2. Filters — country, source, sort
  3. Active-inbox KPI strip — open decisions / already sent / already skipped
  4. Inbox table — one row per keyword, with the numbers that matter for a
     yes/no: volume, est. CPC, competitors, differentiation, competitor
     price, AliExpress price, margin.
  5. Per-row action buttons that sit *under* the table, acting on whichever
     row the user selected in the "decision" column. Streamlit doesn't give
     us buttons inside dataframe cells, so this is the next-best approximation.
     Actions: Send to sourcing / Archive / Enrich.
  6. Detail drawer — only shown when a keyword is picked; renders the
     top-3 AliExpress matches and supporting metrics.
  7. Archived + Already-sent views — each a collapsed expander so they
     don't clutter the active-inbox view, but are one click away when the
     user wants to un-archive or see what they already pushed through.

No tabs. The previous tabbed version ("Results / Manual / Discovery") made
the user hop between three screens to do one workflow — the new flow is a
single column down the page.
"""

# Future annotations keeps PEP-604 union syntax (`float | None`) happy on
# Python 3.9, which is what this dev box runs. Without it, any annotation
# like `float | None` raises at *parse* time because the `|` is evaluated
# eagerly — `from __future__ import annotations` makes every annotation a
# lazy string instead, which the 3.9 parser happily accepts.
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime
import json
import logging
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _country_codes(countries_list) -> list[str]:
    codes = []
    for c in countries_list:
        if isinstance(c, dict):
            codes.append(c.get("code", "DE"))
        elif isinstance(c, str) and len(c) == 2:
            codes.append(c)
    return codes or ["DE"]


def _country_language(countries_list, code: str) -> str:
    for c in countries_list:
        if isinstance(c, dict) and c.get("code") == code:
            return c.get("language", "de")
    return "de"


def _fmt_eur(value, missing: str = "—") -> str:
    try:
        v = float(value or 0)
    except (TypeError, ValueError):
        return missing
    if v == 0:
        return missing
    return f"€{v:.2f}"


def _fmt_int(value, missing: str = "—") -> str:
    try:
        v = int(float(value or 0))
    except (TypeError, ValueError):
        return missing
    return f"{v:,}" if v else missing


def _margin_pct(selling: float, cost: float) -> float | None:
    """Rough gross margin % if we have both numbers, else None."""
    try:
        s, c = float(selling or 0), float(cost or 0)
    except (TypeError, ValueError):
        return None
    if s <= 0 or c <= 0:
        return None
    return (s - c) / s


# ---------------------------------------------------------------------------
# Economics helpers for the inbox decision columns
# ---------------------------------------------------------------------------
#
# The inbox shows three derived numbers per row so the user can make the
# "push to sourcing?" call without mental arithmetic:
#
#   Gross €  = comp_price − ali_price               (absolute profit/sale)
#   Ad €     = est_cpc / assumed_cvr                (ad cost per sale)
#   Net %    = (gross_eur − ad_eur) / comp_price    (the real decision metric)
#
# Net % is what actually matters for Tier C: a 63% gross margin on a €115
# product sounds great, but at €0.52 CPC × 2% CVR you burn ~€26/sale on
# ads, so net margin is closer to 41%. Gross is vanity, net is sanity.
#
# All three columns recompute reactively from the edited dataframe, so
# the numbers update the instant the user tabs out of the Ali € cell —
# they don't wait for the save-to-Sheet round-trip.


def _ad_cost_per_sale(cpc: float, cvr: float) -> float:
    """Estimated ad spend per sale: CPC × (clicks-to-sale) = CPC / CVR."""
    try:
        c = float(cpc or 0)
        r = float(cvr or 0)
    except (TypeError, ValueError):
        return 0.0
    if r <= 0:
        return 0.0
    return c / r


def _net_signal_str(net_pct: float) -> str:
    """Color-code net margin for at-a-glance go/no-go."""
    if net_pct is None or (isinstance(net_pct, float) and (net_pct != net_pct)):
        # NaN check without pulling in math.isnan for the 3.9 typing path
        return "—"
    pct = float(net_pct) * 100
    if net_pct >= 0.35:
        emoji = "🟢"
    elif net_pct >= 0.20:
        emoji = "🟡"
    else:
        emoji = "🔴"
    return f"{emoji} {pct:+.0f}%"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.title("Research")
    st.caption(
        "Keyword inbox — every row below is a product idea waiting for a "
        "decision. Send the good ones to sourcing, archive the rest."
    )

    # Align the optional row-level buttons vertically with their table.
    st.markdown(
        "<style>div[data-testid='stHorizontalBlock']{align-items:center;}</style>",
        unsafe_allow_html=True,
    )

    try:
        from src.core.config import AppConfig
        from src.core.models import ActionLog, ActionType, Product, ProductStatus
        from src.research.pipeline import ResearchPipeline
        import src.sheets.manager as _sheets_mgr
        from src.sheets.manager import get_data_store

        # Defensive schema patch — old running modules may pre-date the
        # aliexpress_top3_json / status columns. These are no-ops on fresh starts.
        for extra in ("aliexpress_top3_json", "status"):
            if extra not in _sheets_mgr.KEYWORD_HEADERS:
                _sheets_mgr.KEYWORD_HEADERS.append(extra)

        store = get_data_store()
        config = AppConfig()
        # Merge Sheet-stored overrides into the singleton so user-changed
        # settings (keywords_per_run, max_competitors, min/max selling
        # price, etc.) actually take effect for Discover runs kicked off
        # from this page. Without this merge, the pipeline falls back to
        # defaults.yaml and the user's sliders are silently ignored —
        # which is how a "10 keywords per run" setting turned into a 150-
        # keyword DataForSEO bill. Fail-open: a Sheets hiccup shouldn't
        # block the user from kicking off Discover, we just log and carry
        # on with defaults.
        try:
            sheet_config = store.get_config()
            if sheet_config:
                config.merge_sheet_config(sheet_config)
        except Exception as _merge_err:
            import logging as _log
            _log.getLogger(__name__).warning(
                "Couldn't merge Sheet config into AppConfig: %s — using defaults",
                _merge_err,
            )
    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    # ----- Top bar: the only two ways to add new rows to the inbox --------
    top_left, top_add_discover, top_add_manual = st.columns([5, 2, 2])
    with top_add_discover:
        discover_clicked = st.button(
            "🔬 Discover more",
            help="Run the full AI discovery pipeline and add new keywords to this inbox.",
            use_container_width=True,
            type="primary",
        )
    with top_add_manual:
        if st.button(
            "➕ Add manually",
            help="Add one keyword you already know you want to research.",
            use_container_width=True,
        ):
            st.session_state["show_manual_form"] = True

    # ----- Discovery run — kicked off here, *executed* in a background ----
    # thread so the pipeline keeps chugging even if the user navigates to a
    # different page. The live status panel below picks up from the module-
    # level registry on every rerun, so the run is "attached" whenever the
    # user is on this page and silently running otherwise.
    if discover_clicked:
        _start_background_discovery(store, config)

    _render_discovery_status_panel()

    # ----- Manual add (inline form, collapsible) --------------------------
    if st.session_state.get("show_manual_form"):
        with st.expander("Add a keyword manually", expanded=True):
            _render_manual_form(store, config, _country_codes(config.countries))

    # ----- Filters --------------------------------------------------------
    st.divider()
    f1, f2, f3, f4 = st.columns([1, 1, 1, 1])
    with f1:
        country = st.selectbox(
            "Country",
            options=["All"] + _country_codes(config.countries),
            key="res_filter_country",
        )
    with f2:
        source = st.selectbox(
            "Source",
            options=["All", "ai", "manual"],
            key="res_filter_source",
        )
    with f3:
        sort_by = st.selectbox(
            "Sort by",
            options=[
                "Newest first",
                "Volume (high → low)",
                "CPC (low → high)",
                "Differentiation (high → low)",
            ],
            key="res_sort",
        )
    with f4:
        search_q = st.text_input(
            "Search keyword",
            key="res_search",
            placeholder="filter by text…",
        )

    # ----- Load inbox -----------------------------------------------------
    # Pull EVERYTHING once then partition — avoids three sheet round-trips.
    all_kws = store.get_keywords(country=country if country != "All" else None)
    if source != "All":
        all_kws = [k for k in all_kws if k.research_source == source]
    if search_q:
        q = search_q.lower()
        all_kws = [k for k in all_kws if q in k.keyword.lower()]

    active = [k for k in all_kws if (k.status or "") in ("", "active")]
    archived = [k for k in all_kws if k.status == "archived"]
    sent = [k for k in all_kws if k.status == "sent_to_sourcing"]

    # Sort active inbox by user pref
    if sort_by == "Volume (high → low)":
        active.sort(key=lambda k: k.monthly_search_volume, reverse=True)
    elif sort_by == "CPC (low → high)":
        active.sort(key=lambda k: k.estimated_cpc or 9e9)
    elif sort_by == "Differentiation (high → low)":
        active.sort(key=lambda k: k.differentiation_score, reverse=True)
    else:
        active.sort(key=lambda k: k.created_at, reverse=True)

    # ----- KPI strip — only the two actionable numbers -------------------
    # Earlier iterations showed In Inbox + Archived + Promoted + In Sourcing
    # which (a) mixed two sheets (KeywordResearch and Products) in one row
    # and (b) surfaced `Promoted` (keywords sent via Discover) which omits
    # manually-added products — so it was silently wrong by the manual-add
    # count. Both of those caused "why don't the numbers add up" confusion.
    #
    # Strip it to just the two numbers the user acts on:
    #   🗂 In inbox   — keywords waiting for yes/no (KeywordResearch)
    #   ✅ In Sourcing — products waiting on the agent (Products)
    # Archived and Promoted are still available as collapsed expanders
    # further down the page — they're audit info, not action info.
    sourcing_products = store.get_products(
        country=country if country != "All" else None,
        status="sourcing",
    )

    # --- Keyword research (KeywordResearch sheet) -------------------------
    st.markdown("**📋 Keyword research** — ideas waiting for your yes/no")
    col_inbox, _ = st.columns([1, 2])
    with col_inbox:
        st.metric(
            "🗂 In inbox",
            len(active),
            help=(
                "Keyword ideas from the Discover pipeline waiting for "
                "your yes/no. KeywordResearch sheet · status is blank "
                "or `active`."
            ),
        )

    st.write("")

    # --- Sourcing queue (Products + Agent Tasks sheets) -------------------
    st.markdown(
        "**📦 Sourcing queue** — products currently with the sourcing agent"
    )

    # Any result message from a previous sync click, carried across the
    # `st.rerun()` that refreshes the counter. Shown above the metric so
    # it's obviously tied to the sync action.
    if "res_sync_result" in st.session_state:
        msg, kind = st.session_state.pop("res_sync_result")
        if kind == "success":
            st.success(msg)
        elif kind == "info":
            st.info(msg)
        elif kind == "error":
            st.error(msg)

    # Metric on the left, sync button on the right — the button is the
    # call-to-action specifically for this metric.
    col_metric, col_action = st.columns([1, 2])
    with col_metric:
        st.metric(
            "✅ In Sourcing",
            len(sourcing_products),
            help=(
                "Products currently waiting on the agent for a landed "
                "cost. Products sheet · test_status = `sourcing`."
            ),
        )
    with col_action:
        # Spacer so the button sits at roughly the metric baseline.
        st.write("")
        if sourcing_products:
            # Streamlit Cloud doesn't run the scheduler daemon, so `pending`
            # rows on Agent Tasks never flip to `processed` automatically
            # after the agent fills in `landed_cost`. This button kicks
            # `job_poll_agent_costs` on-demand so the user doesn't have to
            # hop to the Logs page. The session_state flag + st.rerun()
            # pattern is there so the success message survives the rerun
            # that refreshes the counter.
            if st.button(
                "🔄 Sync with agent sheet",
                help=(
                    "Pick up any landed_cost values the agent has filled in "
                    "on the Agent Tasks sheet. Products with costs move on "
                    "to testing (or killed) and their Agent Tasks row flips "
                    "from `pending` to `processed`."
                ),
                key="res_sync_agent_costs",
            ):
                with st.spinner("Checking Agent Tasks sheet…"):
                    try:
                        # Count awaiting-cost products *before* the job
                        # runs — the job doesn't return anything, so this
                        # is the only way to tell the user what it did.
                        ready_before = store.get_products_awaiting_cost()
                        from src.scheduler.jobs import JobScheduler
                        scheduler = JobScheduler(store)
                        scheduler.job_poll_agent_costs()
                        n = len(ready_before)
                        if n > 0:
                            word = "product" if n == 1 else "products"
                            st.session_state["res_sync_result"] = (
                                f"✅ Processed {n} {word} with new landed "
                                "costs — they've moved on to testing/killed "
                                "and the Agent Tasks rows are now marked "
                                "`processed`.",
                                "success",
                            )
                        else:
                            st.session_state["res_sync_result"] = (
                                "No new landed costs found. The agent "
                                "hasn't filled in any `landed_cost` cells "
                                "yet — fill them in on the sheet and click "
                                "this again.",
                                "info",
                            )
                    except Exception as e:
                        logger.exception("Agent cost sync failed")
                        st.session_state["res_sync_result"] = (
                            f"Sync failed: {type(e).__name__}: {e}",
                            "error",
                        )
                st.rerun()

    # Why the Google Sheet row count can be higher than "In Sourcing":
    # Agent Tasks keeps both `pending` rows (still with the agent) and
    # `processed` rows (agent already filled in landed_cost and the
    # system moved the product on to testing/killed). "In Sourcing"
    # only counts the first group. Spell this out inline so the user
    # doesn't have to reverse-engineer it.
    st.caption(
        "ℹ️ The **Agent Tasks** Google Sheet can have **more** rows than "
        "this number. `processed` rows stay in the sheet as history after "
        "the agent prices them — those products have already moved on to "
        "*testing* or *killed* (see the Products page). `In Sourcing` only "
        "counts rows still `pending`."
    )

    # ----- Inbox table ----------------------------------------------------
    st.subheader("Inbox")
    if not active:
        st.info(
            "No active keywords. Click **🔬 Discover more** to run the AI "
            "pipeline, or **➕ Add manually** to add one by hand."
        )
    else:
        _render_inbox(store, config, active)

    # ----- Archive + Already-sent (collapsed by default) ------------------
    st.divider()
    with st.expander(f"📦 Archive ({len(archived)})"):
        if archived:
            _render_light_table(archived, action_label="Un-archive", key_prefix="unarch",
                                on_action=lambda kid: store.update_keyword(kid, {"status": ""}))
        else:
            st.caption("Nothing archived yet.")

    # Keyword-level audit trail — only tracks the Discover → Sourcing path.
    # Manually-added products don't appear here (they never had a keyword row)
    # which is why this count can be lower than the "In Sourcing" KPI above.
    with st.expander(f"📤 Promoted keywords ({len(sent)})"):
        st.caption(
            "Keywords you've clicked Send to sourcing on. Each one became "
            "a product on the Products sheet — some may already be testing "
            "or killed. Manually-added products aren't listed here."
        )
        if sent:
            _render_light_table(sent, action_label=None, key_prefix="sent")
        else:
            st.caption("No keywords have been promoted via Discover yet.")

    # ----- Rejected keywords ("why did these fail?") ----------------------
    # Gives the human transparency on every keyword the discover pipeline
    # killed, and gives the pipeline itself a deduplication source (so it
    # never re-ideates and re-pays DataForSEO/SerpAPI for the same dead
    # ends). The reads are scoped to the current country filter so a DE
    # operator isn't wading through US drops.
    _render_drops_panel(store, country)

    # ----- Background-run auto-refresh tick --------------------------------
    # _render_discovery_status_panel() sets this flag when a Discovery run
    # is in progress so the status card at the top updates every ~3s. We
    # do the sleep+rerun HERE (at the bottom of the page) so that the
    # inbox table, drops panel, and everything else has a chance to render
    # first — otherwise the user loses their list while a run is active.
    if st.session_state.pop("_discovery_refresh_tick", False):
        import time
        time.sleep(3)
        st.rerun()


# ---------------------------------------------------------------------------
# Inbox table with inline decision actions
# ---------------------------------------------------------------------------

def _render_inbox(store, config, keywords):
    """
    Render the active inbox as a data_editor with a tick column, then
    action buttons underneath. Multi-select is intentional: sending 10
    keywords to sourcing at once is a real user flow.
    """
    # ------------------------------------------------------------------
    # Conversion-rate assumption for the Net % column. Defaults to the
    # value in config (typically 1–2%), but the user can dial it inline
    # to stress-test a pessimistic vs. optimistic scenario without
    # touching defaults.yaml. Persisted in session_state so it survives
    # reruns within a session but resets on reload (intentional — this
    # is a "what-if" knob, not a permanent setting).
    # ------------------------------------------------------------------
    default_cvr_pct = float(getattr(config, "assumed_conversion_rate", 0.02) or 0.02) * 100
    if default_cvr_pct <= 0:
        default_cvr_pct = 2.0
    assumed_cvr_pct = st.session_state.get("inbox_cvr_pct", default_cvr_pct)

    c1, c2 = st.columns([1, 4])
    with c1:
        assumed_cvr_pct = st.number_input(
            "Assumed CVR %",
            min_value=0.1,
            max_value=20.0,
            value=float(assumed_cvr_pct),
            step=0.25,
            key="inbox_cvr_pct",
            help="Assumed conversion rate for the Net % column. "
                 "Ad cost per sale = CPC / CVR. 1–2% is typical for cold traffic on a test page; "
                 "3–5% is realistic for a dialled-in store.",
        )
    cvr = float(assumed_cvr_pct) / 100.0

    # ------------------------------------------------------------------
    # Build the base dataframe from stored Sheet state.
    # ------------------------------------------------------------------
    rows = []
    for k in keywords:
        encoded = quote_plus(k.keyword or "")
        google_link = k.google_shopping_url or f"https://google.de/search?tbm=shop&q={encoded}"
        ali_link = k.aliexpress_url or f"https://www.aliexpress.com/wholesale?SearchText={encoded}"
        # Thumbnail: prefer the Google Shopping first-result image the
        # SerpAPI competition stage captured — it's present for anything
        # that passed the competition filter (~100% hit rate). Fall back
        # to the first AliExpress image when the Google thumb is missing
        # (manual-add rows, legacy keywords pre-dating this field). The
        # AliExpress fallback is a last-resort because it's ~30% hit
        # rate in practice on DE niche product keywords.
        thumb = (getattr(k, "competitor_thumbnail_url", "") or "").strip()
        if not thumb:
            raw_img = (k.aliexpress_image_urls or "").strip()
            thumb = raw_img.split(",")[0].strip() if raw_img else ""
        rows.append({
            "pick": False,
            "thumb": thumb,
            "keyword": k.keyword,
            "country": k.country,
            "volume": int(k.monthly_search_volume or 0),
            "cpc": float(k.estimated_cpc or 0),
            "competitors": int(k.competitor_count or 0),
            "diff": float(k.differentiation_score or 0),
            "comp_price": float(k.median_competitor_price or 0),
            "google_link": google_link,
            "ali_url": ali_link,
            "ali_price": float(k.aliexpress_price or 0),
            # Derived economics columns — filled below AFTER merging
            # any pending cell edits from session_state, so the numbers
            # reflect what the user just typed (not what's saved).
            "gross_eur": float("nan"),
            "ad_eur": float("nan"),
            "net_signal": "—",
            "source": k.research_source,
            "created": k.created_at[:10],
            "_id": k.keyword_id,
        })
    df = pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # REACTIVE MARGIN: merge pending cell edits from st.session_state
    # BEFORE computing derived columns. This makes Gross €, Ad €, and
    # Net % reflect what's in the Ali € cell right now — not the stale
    # stored value. Without this, margin would lag one rerun behind
    # every edit because the row builder reads k.aliexpress_price from
    # the Sheet, which only updates after the save round-trip.
    #
    # Streamlit stashes data_editor pending edits under the widget key
    # as {"edited_rows": {idx: {col: val}}, "added_rows": [...], ...}.
    # ------------------------------------------------------------------
    editor_state = st.session_state.get("inbox_editor", {})
    if isinstance(editor_state, dict):
        pending = editor_state.get("edited_rows", {}) or {}
        for idx_raw, cell_edits in pending.items():
            try:
                idx_int = int(idx_raw)
            except (TypeError, ValueError):
                continue
            if idx_int < 0 or idx_int >= len(df):
                continue
            if not isinstance(cell_edits, dict):
                continue
            for col, val in cell_edits.items():
                if col in df.columns:
                    try:
                        df.at[idx_int, col] = val
                    except Exception:
                        # Type coercion failure — skip, the editor
                        # will show the user's input regardless and
                        # we just won't recompute derived cols for it.
                        pass

    # ------------------------------------------------------------------
    # Compute derived economics from the (possibly edited) Ali € column.
    # ------------------------------------------------------------------
    for idx, row in df.iterrows():
        try:
            comp = float(row.get("comp_price") or 0)
            ali = float(row.get("ali_price") or 0)
            cpc_v = float(row.get("cpc") or 0)
        except (TypeError, ValueError):
            continue
        if comp <= 0 or ali <= 0:
            # No Ali price yet — leave derived cells blank.
            df.at[idx, "gross_eur"] = float("nan")
            df.at[idx, "ad_eur"] = float("nan")
            df.at[idx, "net_signal"] = "—"
            continue
        gross = comp - ali
        ad = _ad_cost_per_sale(cpc_v, cvr)
        net = (gross - ad) / comp if comp > 0 else None
        df.at[idx, "gross_eur"] = gross
        df.at[idx, "ad_eur"] = ad
        df.at[idx, "net_signal"] = _net_signal_str(net)

    edited = st.data_editor(
        df.drop(columns=["_id"]),
        hide_index=True,
        use_container_width=True,
        # Slightly taller rows so the thumbnail image has room to render
        # without looking squashed. Streamlit uses ~35px per row by
        # default; bumping to 56 gives us ~48px of image real estate.
        row_height=56,
        height=min(760, 80 + 56 * len(df)),
        column_config={
            "pick": st.column_config.CheckboxColumn("Pick", width="small",
                help="Tick the rows you want to act on, then use the buttons below."),
            "thumb": st.column_config.ImageColumn(
                "Preview",
                width="small",
                help="AliExpress product thumbnail (if matched). Click to enlarge.",
            ),
            "keyword": st.column_config.TextColumn("Keyword"),
            "country": st.column_config.TextColumn("Country", width="small"),
            "volume": st.column_config.NumberColumn("Volume", format="%d",
                help="Monthly search volume from Keyword Planner."),
            "cpc": st.column_config.NumberColumn("Est. CPC", format="€%.2f",
                help="Estimated cost per click from Keyword Planner."),
            "competitors": st.column_config.NumberColumn("Comps", format="%d",
                help="Unique sellers in Google Shopping."),
            "diff": st.column_config.NumberColumn("Diff", format="%.0f",
                help="Differentiation score (higher = easier to stand out)."),
            "comp_price": st.column_config.NumberColumn("Comp €", format="€%.2f",
                help="Median competitor selling price."),
            "google_link": st.column_config.LinkColumn(
                "Google",
                display_text="🔍 Open",
                width="small",
                help="Open this keyword in Google Shopping (DE) in a new tab.",
            ),
            "ali_url": st.column_config.LinkColumn(
                "AliExpress",
                display_text="🛒 Open",
                width="small",
                help="Open this keyword on AliExpress in a new tab. Pick a supplier and paste its URL + price here.",
            ),
            "ali_price": st.column_config.NumberColumn(
                "Ali €",
                format="€%.2f",
                help="Paste the AliExpress supplier price here. Tick Pick + click Send to sourcing to promote.",
            ),
            "gross_eur": st.column_config.NumberColumn(
                "Gross €",
                format="€%.2f",
                help="Absolute profit per sale BEFORE ads: comp_price − ali_price.",
            ),
            "ad_eur": st.column_config.NumberColumn(
                "Ad €",
                format="€%.2f",
                help=f"Estimated ad spend per sale: CPC ÷ CVR. "
                     f"At the current CVR assumption of {assumed_cvr_pct:.2f}%, "
                     f"this is the cost of acquiring one customer via paid traffic.",
            ),
            "net_signal": st.column_config.TextColumn(
                "Net %",
                width="small",
                help="Net margin after ads: (gross − ad_cost) / comp_price. "
                     "🟢 ≥35% (push), 🟡 20–35% (maybe), 🔴 <20% (pass). "
                     "This is the number that actually decides go/no-go.",
            ),
            "source": st.column_config.TextColumn("Source", width="small"),
            "created": st.column_config.TextColumn("Created", width="small"),
        },
        disabled=[
            "thumb", "keyword", "country", "volume", "cpc", "competitors", "diff",
            "comp_price", "google_link", "gross_eur", "ad_eur", "net_signal",
            "source", "created",
        ],
        key="inbox_editor",
    )

    # ------------------------------------------------------------------
    # Persist edits to Ali €/URL back to the Keywords sheet. The user
    # workflow is: edit here → tick Pick → click Send to sourcing.
    # We do NOT auto-promote — every candidate gets a human review pass,
    # even ones where the pipeline already had an AliExpress match. The
    # persist-on-edit keeps the entered price available to downstream
    # sourcing so a later Send-to-sourcing pass uses the manual price.
    # ------------------------------------------------------------------
    for i, k in enumerate(keywords):
        try:
            new_price = float(edited.iloc[i].get("ali_price") or 0)
        except (TypeError, ValueError):
            new_price = 0.0
        old_price = float(k.aliexpress_price or 0)
        new_url = str(edited.iloc[i].get("ali_url") or "").strip()
        old_url = str(k.aliexpress_url or "").strip()

        # Ignore URL "changes" that are just the pipeline's fallback
        # search URL we generated for display when the keyword has no
        # real AliExpress URL yet — otherwise every render would
        # spuriously "update" every row.
        fallback_search_url = (
            f"https://www.aliexpress.com/wholesale?SearchText={quote_plus(k.keyword or '')}"
        )
        url_changed = bool(new_url) and new_url != old_url and new_url != fallback_search_url

        updates: dict = {}
        if new_price > 0 and abs(new_price - old_price) > 1e-6:
            updates["aliexpress_price"] = new_price
        if url_changed:
            updates["aliexpress_url"] = new_url

        if updates:
            try:
                store.update_keyword(k.keyword_id, updates)
            except Exception as exc:
                logger.exception("Failed to persist inbox edits for %s: %s", k.keyword_id, exc)
                st.error(f"Could not save edits for {k.keyword}: {exc}")

    picked_ids = [
        keywords[i].keyword_id
        for i in range(len(keywords))
        if edited.iloc[i].get("pick", False)
    ]
    n_picked = len(picked_ids)

    # Action row under the table.
    a1, a2, a3, a4 = st.columns([1.6, 1.3, 1.3, 1])
    with a1:
        st.caption(f"{n_picked} selected" if n_picked else "No rows selected")
    with a2:
        send_click = st.button(
            f"⚡ Send to Agent ({n_picked})" if n_picked else "⚡ Send to Agent",
            type="primary",
            use_container_width=True,
            disabled=n_picked == 0,
            help=(
                "Promote picked rows. If Ali € is filled, the product skips "
                "sourcing and lands in READY_TO_TEST (you already sourced it). "
                "If Ali € is empty, it goes to SOURCING so the agent finds "
                "the AliExpress match first."
            ),
        )
    with a3:
        skip_click = st.button(
            f"🗑 Kill ({n_picked})" if n_picked else "🗑 Kill",
            use_container_width=True,
            disabled=n_picked == 0,
            help="Archive the keyword. Hidden from the inbox but kept so the pipeline never re-suggests it.",
        )
    with a4:
        enrich_click = st.button(
            "🔬 Enrich",
            use_container_width=True,
            disabled=n_picked == 0,
            help="Re-run competition + AliExpress lookup on the picked rows.",
        )

    if send_click and picked_ids:
        _send_to_agent(store, config, keywords, picked_ids)
        st.rerun()
    if skip_click and picked_ids:
        for kid in picked_ids:
            store.update_keyword(kid, {"status": "archived"})
        st.success(f"Archived {len(picked_ids)} keyword(s).")
        st.rerun()
    if enrich_click and picked_ids:
        _enrich_selected(store, config, picked_ids)
        st.rerun()

    # Detail drawer — appears once the user picks exactly one row.
    if n_picked == 1:
        kid = picked_ids[0]
        kw = next((k for k in keywords if k.keyword_id == kid), None)
        if kw:
            _render_detail_drawer(kw)


def _render_detail_drawer(kw):
    """Details for a single keyword: metrics, top-3 AliExpress matches, links."""
    st.divider()
    st.subheader(f"🔍 {kw.keyword}")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Volume", f"{kw.monthly_search_volume:,}")
        st.metric("Est. CPC", _fmt_eur(kw.estimated_cpc))
    with c2:
        st.metric("Competitors", kw.competitor_count or 0)
        st.metric("Differentiation", f"{kw.differentiation_score:.0f}/100")
    with c3:
        st.metric("Competition", (kw.competition_level or "—").title())
        st.metric("Competition type", (kw.competition_type or "—").replace("_", " ").title())
    with c4:
        st.metric("Competitor price (median)", _fmt_eur(kw.median_competitor_price))
        st.metric("AliExpress match", _fmt_eur(kw.aliexpress_price))

    # Top-3 AliExpress cards ------------------------------------------------
    top3 = []
    raw = getattr(kw, "aliexpress_top3_json", "") or ""
    if raw:
        try:
            top3 = json.loads(raw)
        except (ValueError, TypeError):
            top3 = []

    if top3:
        from dashboard.components.widgets import render_image_download

        st.markdown("**Top 3 AliExpress matches**")
        cols = st.columns(len(top3))
        for idx, (col, item) in enumerate(zip(cols, top3)):
            with col:
                tag = item.get("tag", "")
                colour = {"Best Seller": "orange", "Best Price": "green", "Best Rated": "blue"}.get(tag, "gray")
                st.markdown(f":{colour}[**{tag or '—'}**]")
                if item.get("image_url"):
                    st.image(item["image_url"], width=120)
                title = item.get("title", "")
                if title:
                    st.caption(title[:90] + ("…" if len(title) > 90 else ""))
                st.markdown(
                    f"Price: **{_fmt_eur(item.get('price'))}**  \n"
                    f"Rating: {item.get('rating', 0):.1f} / 5  \n"
                    f"Orders: {_fmt_int(item.get('orders'))}"
                )
                margin_pct = item.get("margin_pct") or 0
                if margin_pct:
                    pct = margin_pct * 100
                    if pct >= 30:
                        st.success(f"Est. margin {pct:.0f}%")
                    elif pct >= 15:
                        st.warning(f"Est. margin {pct:.0f}%")
                    else:
                        st.error(f"Est. margin {pct:.0f}%")
                if item.get("url"):
                    st.markdown(f"[View on AliExpress]({item['url']})")
                # One-click image export — the whole point of the Research
                # drawer for most users is to find images they can drop
                # into AliExpress camera-search for alternative suppliers.
                if item.get("image_url"):
                    render_image_download(
                        item["image_url"],
                        f"kw_{kw.keyword_id}",
                        idx,
                    )

    # Google Shopping thumbnail + download (falls outside top-3 because
    # the thumb comes from SerpAPI's competition stage, not AliExpress).
    gs_thumb = (getattr(kw, "competitor_thumbnail_url", "") or "").strip()
    if gs_thumb:
        from dashboard.components.widgets import render_image_download

        st.markdown("**Google Shopping thumbnail**")
        tcol, _ = st.columns([1, 3])
        with tcol:
            try:
                st.image(gs_thumb, width=140)
            except Exception:
                st.caption("(preview failed)")
            render_image_download(gs_thumb, f"kw_{kw.keyword_id}", 99)

    # Links + notes --------------------------------------------------------
    st.markdown("**Links**")
    encoded = kw.keyword.replace(" ", "+")
    gs = kw.google_shopping_url or f"https://google.de/search?tbm=shop&q={encoded}"
    ali = kw.aliexpress_url or f"https://www.aliexpress.com/wholesale?SearchText={encoded}"
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"🔍 [Google Shopping]({gs})")
    with c2:
        st.markdown(f"🛒 [AliExpress]({ali})")
    if kw.notes:
        st.info(f"Notes: {kw.notes}")


# ---------------------------------------------------------------------------
# Send-to-agent promotion
# ---------------------------------------------------------------------------

def _send_to_agent(store, config, keywords, keyword_ids):
    """Promote picked keywords to the agent's queue — smart about status.

    This is the single hand-off from Research → Products. We *used* to
    unconditionally create SOURCING rows; now the status depends on
    whether the user has already sourced the AliExpress match by hand in
    the Inbox:

      - Ali € filled  → READY_TO_TEST. User did the sourcing; the agent
                        skips that step and moves straight to listing/test.
      - Ali € empty   → SOURCING. User wants the agent to find the
                        AliExpress match first.

    We always run EconomicValidator.calculate_economics() to populate the
    derived fields (break-even CPA, test budget, margin, etc.) the agent
    needs downstream — but we do NOT use its pass/fail to gate promotion.
    The user already made the go/no-go call in the Inbox by looking at
    Net % before picking the row. The economics fields are written
    regardless so every product has consistent metadata on arrival.

    Upsert behaviour: if a Product already exists for this keyword_id —
    e.g. from the deprecated Manual Review flow which created
    PENDING_MANUAL_REVIEW rows — we UPDATE it in place rather than
    creating a duplicate. This is how legacy PMR products get migrated
    organically: the user fills Ali € in the Inbox and hits Send, and
    the old PMR row becomes a fresh READY_TO_TEST row.

    The keyword row isn't deleted — just flagged `status=sent_to_sourcing`
    so the Inbox hides it and the agent's dedup logic never re-suggests it.
    """
    from src.core.models import ActionLog, ActionType, Product, ProductStatus
    from src.economics.validator import EconomicValidator

    kw_by_id = {k.keyword_id: k for k in keywords}

    # Preload the full product list once and index by keyword_id so we can
    # decide upsert-vs-create per row without N round-trips to the sheet.
    all_products = store.get_products()
    by_kid: dict = {}
    for p in all_products:
        if p.keyword_id:
            by_kid[p.keyword_id] = p

    validator = EconomicValidator(config=config)

    ready = 0   # promoted to READY_TO_TEST (already sourced)
    sourcing = 0  # sent to SOURCING (agent to source)

    for kid in keyword_ids:
        kw = kw_by_id.get(kid)
        if not kw:
            continue

        ali_filled = float(kw.aliexpress_price or 0) > 0
        target_status = (
            ProductStatus.READY_TO_TEST.value
            if ali_filled
            else ProductStatus.SOURCING.value
        )

        existing = by_kid.get(kid)
        promote_reason = (
            "Promoted from Research inbox (user already sourced AliExpress)"
            if ali_filled
            else "Promoted from Research inbox (agent to find AliExpress match)"
        )

        if existing:
            # ── Upsert branch: reuse the existing Product row ──────────
            # Overlay inbox-entered fields onto the existing product
            # before we compute economics — validator reads these off
            # the Product object.
            existing.aliexpress_url = kw.aliexpress_url or existing.aliexpress_url
            existing.aliexpress_price = (
                kw.aliexpress_price if ali_filled else existing.aliexpress_price
            )
            # Landed cost = AliExpress price (best proxy we have until
            # shipping is known). Agent can refine on their side.
            existing.landed_cost = (
                kw.aliexpress_price if ali_filled else existing.landed_cost
            )
            existing.selling_price = (
                kw.median_competitor_price
                or existing.selling_price
                or 0
            )
            existing.test_status = target_status
            existing.reason = promote_reason

            econ = (
                validator.calculate_economics(existing) if ali_filled else {}
            )

            updates = {
                "aliexpress_url": existing.aliexpress_url,
                "aliexpress_price": float(existing.aliexpress_price or 0),
                "landed_cost": float(existing.landed_cost or 0),
                "selling_price": float(existing.selling_price or 0),
                "test_status": target_status,
                "reason": promote_reason,
                **econ,
            }
            try:
                store.update_product(existing.product_id, updates)
                product_id = existing.product_id
            except Exception as exc:
                logger.exception("update_product failed for %s: %s", kid, exc)
                st.error(f"Could not promote {kw.keyword}: {exc}")
                continue
        else:
            # ── Create branch: fresh Product row ───────────────────────
            landed = float(kw.aliexpress_price or 0) if ali_filled else 0.0
            product = Product(
                keyword_id=kw.keyword_id,
                keyword=kw.keyword,
                country=kw.country,
                language=kw.language,
                monthly_search_volume=kw.monthly_search_volume,
                estimated_cpc=kw.estimated_cpc,
                competition_level=kw.competition_level,
                competitor_count=kw.competitor_count,
                differentiation_score=kw.differentiation_score,
                competition_type=kw.competition_type,
                google_shopping_url=kw.google_shopping_url,
                competitor_pdp_url=kw.competitor_pdp_url,
                aliexpress_url=kw.aliexpress_url,
                aliexpress_price=kw.aliexpress_price,
                aliexpress_rating=kw.aliexpress_rating,
                aliexpress_orders=kw.aliexpress_orders,
                aliexpress_image_urls=kw.aliexpress_image_urls,
                aliexpress_top3_json=getattr(kw, "aliexpress_top3_json", "") or "",
                selling_price=kw.median_competitor_price or 0,
                landed_cost=landed,
                test_status=target_status,
                reason=promote_reason,
            )
            # Stamp derived economics fields when we already have a cost.
            if ali_filled:
                for k_, v_ in validator.calculate_economics(product).items():
                    setattr(product, k_, v_)
            store.add_product(product)
            product_id = product.product_id

        # Activity log — entry is mostly for audit / history, shows up on
        # the Logs page.
        action_type = (
            ActionType.ECONOMICS_PASSED.value
            if ali_filled
            else ActionType.SOURCING_STARTED.value
        )
        store.add_log(ActionLog(
            product_id=product_id,
            action_type=action_type,
            old_status="",
            new_status=target_status,
            reason=promote_reason,
            country=kw.country,
        ))

        store.update_keyword(kid, {"status": "sent_to_sourcing"})

        # Push the product onto the sourcing agent's work queue. We sync on
        # BOTH branches — previously the ali_filled → READY_TO_TEST branch
        # skipped this on the theory that "the user already sourced, so the
        # agent has nothing to do". But the button is literally called
        # "Send to Agent": if the user clicks it, they want the product
        # visible to the agent, full stop. The agent can quickly process
        # rows that already have aliexpress_price filled (they just confirm
        # landed_cost = aliexpress_price) — that's cheaper than explaining
        # to the user why the button silently skipped half their picks.
        #
        # Previously this sync only happened at the end of a Discovery run
        # (pipeline.py _finalize) and the nightly scheduler — which meant
        # clicking "Send to Agent" from the dashboard wrote the Product row
        # but never queued the task, leaving the agent's sheet empty until
        # the next full run. Doing it inline here closes that gap.
        try:
            product_obj = (
                existing
                if existing
                else product  # the fresh Product built above
            )
            store.sync_product_to_agent_tasks(product_obj)
        except Exception as sync_err:
            # Agent Tasks sync is best-effort — the product is already
            # in the Products tab, so the user can still find it. Log
            # and continue so one sheet hiccup doesn't block the batch.
            logger.error(
                "sync_product_to_agent_tasks failed for %s: %s",
                product_id, sync_err,
            )

        if ali_filled:
            ready += 1
        else:
            sourcing += 1

    # Compose a single success line describing both branches.
    parts = []
    if ready:
        parts.append(f"**{ready}** → READY_TO_TEST (already sourced)")
    if sourcing:
        parts.append(f"**{sourcing}** → SOURCING (agent to source)")
    if parts:
        st.success("Sent to agent: " + " · ".join(parts) + ". Check the **Products** page.")


# Alias kept for any callers that still reference the old name. Safe to
# delete once the Research page is the only caller.
_send_to_sourcing = _send_to_agent


# ---------------------------------------------------------------------------
# Enrich (re-run competition + AliExpress on existing rows)
# ---------------------------------------------------------------------------

def _enrich_selected(store, config, keyword_ids):
    from src.core.config import ALIEXPRESS_APP_KEY
    from src.research.pipeline import ResearchPipeline

    if not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_"):
        st.warning(
            "AliExpress credentials not set — competition analysis will run, "
            "but AliExpress data will be skipped."
        )

    pipeline = ResearchPipeline(store, config)
    with st.spinner(f"Re-running competition analysis on {len(keyword_ids)} keyword(s)…"):
        stats = pipeline.enrich_keywords(keyword_ids, run_aliexpress=True)
    st.success(
        f"Enriched {stats['enriched_count']} keyword(s). "
        f"AliExpress matched: {stats['aliexpress_matched_count']}."
    )
    for err in stats.get("errors", []):
        st.error(err)


# ---------------------------------------------------------------------------
# Manual-add form (shown/hidden via session state)
# ---------------------------------------------------------------------------

def _render_manual_form(store, config, country_codes):
    with st.form("manual_keyword_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            mk_kw = st.text_input("Keyword *", placeholder="e.g., kabellose bluetooth kopfhörer")
            mk_country = st.selectbox("Country *", options=country_codes)
            mk_language = st.text_input("Language", value="de")
        with col2:
            mk_volume = st.number_input("Monthly search volume", min_value=0, value=0, step=100)
            mk_cpc = st.number_input("Estimated CPC (EUR)", min_value=0.0, value=0.0, step=0.05)
            mk_notes = st.text_area("Notes", placeholder="Source, reasoning, anything worth remembering")

        c1, c2 = st.columns([1, 3])
        with c1:
            submitted = st.form_submit_button("Add to inbox", type="primary")
        with c2:
            cancel = st.form_submit_button("Cancel")

        if cancel:
            st.session_state["show_manual_form"] = False
            st.rerun()

        if submitted:
            if not mk_kw:
                st.error("Keyword is required.")
            else:
                from src.research.pipeline import ResearchPipeline
                pipeline = ResearchPipeline(store, config)
                pipeline.add_manual_keyword(
                    keyword=mk_kw, country=mk_country, language=mk_language,
                    monthly_search_volume=mk_volume, estimated_cpc=mk_cpc, notes=mk_notes,
                )
                st.session_state["show_manual_form"] = False
                st.success(f"Added '{mk_kw}' to the inbox.")
                st.rerun()


# ---------------------------------------------------------------------------
# Discovery run — background-thread model
# ---------------------------------------------------------------------------
# The old inline `_run_discovery` blocked the page render for the entire run,
# which meant clicking any other nav item killed the UI feedback (the thread
# kept going, but the user had no way to see progress). We now dispatch to a
# module-level registry (`dashboard.background_runs`) and render a live
# status panel that picks up whatever's active whenever the user is on this
# page. The thread is `daemon=True` so it won't hang the dashboard on exit.

def _start_background_discovery(store, config):
    """Kick off a Discover run in a background thread. Returns immediately.
    The run_id is stashed in session state so this page (and any other that
    chooses to surface it) can render progress on subsequent reruns."""
    from dashboard import background_runs

    codes = _country_codes(config.countries)
    if not codes:
        st.warning("No countries configured — add one in Settings first.")
        return

    # If there's already a run going, tell the user — don't pile up parallel
    # runs that would blow through DataForSEO quota twice.
    active = background_runs.get_active_runs()
    if active:
        st.warning(
            f"A discovery run is already in progress (started "
            f"{int(active[0].elapsed_seconds)}s ago). Wait for it to finish."
        )
        return

    run_id = background_runs.start_discovery(store, config, codes)
    st.session_state["active_discovery_run_id"] = run_id
    # Opportunistic GC so the registry dict doesn't grow forever in a
    # long-lived dashboard process.
    background_runs.clear_finished(older_than_seconds=3600)
    st.toast(f"🔬 Discovery started ({', '.join(codes)})", icon="🔬")


def _render_discovery_status_panel():
    """Render the status of the most recent / active background Discover run.

    Three cases:
      1. No run in session → render nothing.
      2. Run still active → show progress card + auto-rerun every 3s.
      3. Run finished (done/error) → show funnel + cost block, then clear
         the session pointer once the user has seen the results.
    """
    from dashboard import background_runs

    run_id = st.session_state.get("active_discovery_run_id")
    if not run_id:
        return

    state = background_runs.get_run(run_id)
    if state is None:
        # Registry was cleared (process restart, or cleanup aged it out).
        # Drop the stale pointer so we don't keep rendering nothing.
        st.session_state.pop("active_discovery_run_id", None)
        return

    # --- Active run: live progress card --------------------------------
    if state.is_active:
        elapsed = int(state.elapsed_seconds)
        done_countries = len(state.all_stats)
        total_countries = len(state.country_codes)
        # Only show the country counter when >1 country is queued, otherwise
        # "(0/1 countries, 9s elapsed)" just buries the important info (which
        # step we're on and how many keywords are done). For a single-country
        # run we collapse to just the elapsed time.
        if total_countries > 1:
            suffix = f" ({done_countries}/{total_countries} countries, {elapsed}s elapsed)"
        else:
            suffix = f" ({elapsed}s elapsed)"
        # Summarise the most-recent country's partial stats so the user can
        # see the pipeline is doing something, not just spinning.
        last_stats = state.all_stats[-1] if state.all_stats else None
        with st.status(
            f"🔬 Discovery running — {state.progress_msg}{suffix}",
            expanded=True,
            state="running",
        ):
            st.caption(
                "This runs in a background thread — you can navigate to other "
                "pages and come back; the run will keep going."
            )
            # Render the native progress bar when we have a real counter
            # (competition or AliExpress loop). For batched stages
            # (ideation, volume check, Claude QA, write) we skip it —
            # showing a 0-width bar just looks stuck.
            if state.total > 0:
                st.progress(
                    state.current / max(state.total, 1),
                    text=f"{state.current_stage} — {state.current} / {state.total} keywords",
                )
            if last_stats:
                st.write(
                    f"**Last finished:** {last_stats.get('country', '?')} — "
                    f"generated {last_stats.get('keywords_generated', 0)}, "
                    f"passed volume {last_stats.get('keywords_passed_volume', 0)}, "
                    f"added {last_stats.get('products_added_to_sourcing', 0)}"
                )
        # Auto-rerun is handled at the END of main() (after the inbox
        # table renders) — not here. Doing sleep+rerun inline would abort
        # the script and hide everything below the panel, which is why
        # users saw the inbox disappear while a Discovery run was active.
        # Flag it so main() knows to schedule a refresh tick after its
        # other content is on screen.
        st.session_state["_discovery_refresh_tick"] = True
        return

    # --- Finished: render final funnel + cost block --------------------
    if state.status == "error":
        st.error(
            f"Discovery failed after {int(state.elapsed_seconds)}s. "
            f"Message: {state.error.splitlines()[0] if state.error else '?'}"
        )
        with st.expander("Full traceback", expanded=False):
            st.code(state.error or "(no traceback captured)")
        # One-shot: clear the pointer so the panel doesn't keep screaming
        # about the same error on every rerun.
        st.session_state.pop("active_discovery_run_id", None)
        return

    # status == "done"
    #
    # The banner reports BOTH lanes explicitly:
    #   - Sourcing: matched on AliExpress → Products (ready for the agent)
    #   - Inbox:    unmatched → KeywordResearch (manual AliExpress lookup)
    #
    # Old version only showed `total_added` = matched lane only, which made
    # a run that wrote 6 inbox rows look like "0 new keywords added" —
    # actively misleading. The user was right to call this out.
    country_word = "y" if len(state.country_codes) == 1 else "ies"
    sourcing = state.total_added_sourcing
    inbox = state.total_added_inbox
    if sourcing + inbox == 0:
        # Pipeline completed cleanly but nothing survived all filters.
        st.success(
            f"✅ Discovery complete in {int(state.elapsed_seconds)}s "
            f"across {len(state.country_codes)} countr{country_word} — "
            "no new keywords made it through the filters."
        )
    else:
        # Show both lanes so "0 to Sourcing but 6 to Inbox" is visible.
        sourcing_word = "product" if sourcing == 1 else "products"
        inbox_word = "keyword" if inbox == 1 else "keywords"
        st.success(
            f"✅ Discovery complete in {int(state.elapsed_seconds)}s "
            f"across {len(state.country_codes)} countr{country_word}:\n\n"
            f"- **{sourcing}** {sourcing_word} added to **Sourcing** "
            f"(matched on AliExpress)\n"
            f"- **{inbox}** {inbox_word} added to **Research Inbox** "
            f"(need manual AliExpress lookup)"
        )
    _render_funnel_block(state.all_stats)
    _render_cost_block(state.all_stats)
    if sourcing + inbox == 0:
        st.warning(
            "Nothing made it to the sheet. Check the funnel above for the "
            "biggest killer stage, or the **Logs** view for full detail."
        )
    # Clear the active-run pointer so next page load doesn't re-render the
    # same completed run. Results are still accessible via the Keywords
    # inbox below and the Research Drops tab.
    st.session_state.pop("active_discovery_run_id", None)


def _render_cost_block(all_stats):
    """Aggregated API-spend summary for one completed Discover run. Extracted
    so both the live-completion path and any future history view can reuse
    it."""
    total_cost = sum(float(s.get("cost_total_usd", 0) or 0) for s in all_stats)
    if total_cost <= 0:
        return
    st.info(f"**Run cost: ${total_cost:.4f} USD**  (persisted to *API Costs* sheet)")
    with st.expander("Cost breakdown", expanded=False):
        agg: dict[tuple, dict] = {}
        for s in all_stats:
            for row in (s.get("cost_breakdown") or []):
                key = (row.get("provider", ""), row.get("endpoint", ""))
                if key not in agg:
                    agg[key] = {
                        "Provider": row.get("provider", ""),
                        "Endpoint": row.get("endpoint", ""),
                        "Calls": 0,
                        "Cost ($)": 0.0,
                        "Estimated": False,
                    }
                agg[key]["Calls"] += int(row.get("calls", 0) or 0)
                agg[key]["Cost ($)"] += float(row.get("cost_usd", 0) or 0)
                agg[key]["Estimated"] = (
                    agg[key]["Estimated"] or bool(row.get("any_estimated"))
                )
        if agg:
            import pandas as _pd
            breakdown_df = _pd.DataFrame(sorted(
                agg.values(), key=lambda r: r["Cost ($)"], reverse=True
            ))
            breakdown_df["Cost ($)"] = breakdown_df["Cost ($)"].round(4)
            st.dataframe(breakdown_df, hide_index=True, use_container_width=True)
            st.caption(
                "Estimated = no exact cost returned by the provider; shown "
                "at the published plan rate."
            )


# ---------------------------------------------------------------------------
# Funnel block — shows per-stage survival of a Discover run
# ---------------------------------------------------------------------------

def _render_funnel_block(all_stats):
    """
    Render a funnel table summarising the run we just finished.

    The pipeline carries two things we want here:
      • Per-stage survivor counts on `stats` (`keywords_generated`,
        `keywords_passed_volume`, …). These define the *width* of each step.
      • `stats["dropped_keywords"]` — a list of `{keyword, stage, reason}`
        dicts, one per keyword the pipeline killed. We aggregate by stage to
        show *where* the pipeline lost volume and to list a few example
        reasons under each row.

    Multiple countries run in sequence, so we accept `all_stats` (list of
    per-country stat dicts) and aggregate across them. Per-country split is
    one extra click away in the expander below the table.
    """
    if not all_stats:
        return

    # --- Aggregate survivor counts across countries -----------------------
    def _sum(key: str) -> int:
        return sum(int(s.get(key, 0) or 0) for s in all_stats)

    generated   = _sum("keywords_generated")
    dupes       = _sum("duplicates_skipped")
    after_dedup = max(generated - dupes, 0)
    length_pass        = _sum("keywords_passed_length") or after_dedup       # fallback for older runs
    llm_price_pass     = _sum("keywords_passed_llm_price") or length_pass    # same fallback
    llm_quality_pass   = _sum("keywords_passed_llm_quality") or llm_price_pass  # Layer 2 fallback
    # `keywords_with_planner_data` = rows Google Keyword Planner actually
    # had aggregates for. Anything missing is dropped at stage `volume_no_data`
    # rather than polluting the inbox with blank rows.
    with_planner_data  = _sum("keywords_with_planner_data") or llm_quality_pass
    vol_pass           = _sum("keywords_passed_volume")
    cpc_pass           = _sum("keywords_passed_cpc") or vol_pass             # fallback for older runs
    llm_qa_pass        = _sum("keywords_passed_llm_qa") or cpc_pass          # Layer 3 fallback
    comp_pass          = _sum("keywords_passed_competition")
    price_pass  = _sum("keywords_passed_price_filter")
    matched     = _sum("products_matched")
    capped_out  = _sum("products_capped_out")
    written     = _sum("products_added_to_sourcing")
    # Any per-country stat flipping the soft-warn flag counts the whole run.
    soft_warn_triggered = any(
        bool(s.get("products_soft_warn_triggered")) for s in all_stats
    )

    # Early-out: the pipeline never produced anything this run — the
    # funnel would just be a column of zeros. Don't bother.
    if generated == 0 and written == 0:
        return

    # --- Aggregate drops by stage ----------------------------------------
    drops_by_stage: dict[str, list[dict]] = {}
    for s in all_stats:
        for d in (s.get("dropped_keywords") or []):
            drops_by_stage.setdefault(d.get("stage", "?"), []).append(d)

    def _drop_count(stage: str) -> int:
        return len(drops_by_stage.get(stage, []))

    # --- Build the funnel table ------------------------------------------
    # "Dropped here" is the count of keywords killed *at this stage*, not
    # the cumulative loss. `dedup` hits after generation; `volume` after
    # dedup; etc. `aliexpress_soft` is recorded as a drop but the product
    # is still written — show it for transparency, flagged as soft.
    rows = [
        {"Stage": "LLM generated",          "Surviving": generated,   "Dropped here": 0},
        {"Stage": "After dedup",            "Surviving": after_dedup, "Dropped here": _drop_count("dedup")},
        {"Stage": "Passed length filter",      "Surviving": length_pass,      "Dropped here": _drop_count("length")},
        {"Stage": "Passed LLM price check",    "Surviving": llm_price_pass,   "Dropped here": _drop_count("llm_price")},
        {"Stage": "Passed LLM quality check",  "Surviving": llm_quality_pass, "Dropped here": _drop_count("llm_quality")},
        {"Stage": "Has Google Planner data",   "Surviving": with_planner_data, "Dropped here": _drop_count("volume_no_data")},
        {"Stage": "Passed volume filter",      "Surviving": vol_pass,         "Dropped here": _drop_count("volume")},
        {"Stage": "Passed CPC filter",         "Surviving": cpc_pass,         "Dropped here": _drop_count("cpc")},
        {"Stage": "Passed Claude QA (pre-SerpAPI)", "Surviving": llm_qa_pass, "Dropped here": _drop_count("llm_qa")},
        {"Stage": "Passed competition",        "Surviving": comp_pass,        "Dropped here": _drop_count("competition")},
        {"Stage": "Passed price filter",    "Surviving": price_pass,  "Dropped here": _drop_count("price")},
        {"Stage": "Matched on AliExpress",  "Surviving": matched,     "Dropped here": max(price_pass - matched, 0)},
        # Final stage — "Written to Products". When the per-run cap is
        # disabled (default as of 2026-04-24), `capped_out` is always 0
        # and Surviving == Matched. When a non-zero cap is configured
        # and fires, this row shows how many got dropped by the cap.
        # The old separate "Written to sheet" tautology row was removed:
        # it just restated Surviving with Dropped=0.
        {"Stage": "Written to Products",    "Surviving": written,     "Dropped here": capped_out},
    ]

    st.subheader("Research funnel")
    st.caption(
        "Where this run's volume went. **Surviving** is how many keywords "
        "were still in play entering each stage; **Dropped here** is how "
        "many that stage killed."
    )
    # Soft-warn banner: the pipeline sets this when a run promotes more
    # products than `research.max_products_soft_warn` (default 25). The
    # hard cap is disabled by default — we trust the filters — but a run
    # producing 80+ products is almost always a filter regression, not a
    # bumper crop. Surface it loudly without blocking the data.
    if soft_warn_triggered:
        st.warning(
            f"⚠️ **Unusually large batch:** {written} products promoted to "
            "sourcing in this run. Filters are doing their job so these "
            "are all written, but sanity-check your thresholds "
            "(volume / CPC / competition / price) — a batch this size is "
            "often the first sign of a filter regression."
        )
    import pandas as _pd
    funnel_df = _pd.DataFrame(rows)
    # Percent-of-start column helps eyeball which stages are the biggest killers.
    start = max(generated, 1)
    funnel_df["% of start"] = (funnel_df["Surviving"] / start * 100).round(0).astype(int).astype(str) + "%"
    st.dataframe(funnel_df, hide_index=True, use_container_width=True)

    # --- Drop samples — collapsed by default, one expander per stage -----
    # Users want "why did volume kill 120 of my keywords?" answered fast.
    # Show up to 10 example {keyword, reason} pairs per stage, newest first
    # (insertion order preserved on the list). The full drop list is on the
    # Research Drops sheet tab for anyone who wants to paste into Excel.
    total_drops = sum(len(v) for v in drops_by_stage.values())
    if total_drops:
        with st.expander(
            f"🔍 See example drops ({total_drops} across this run)",
            expanded=False,
        ):
            # Soft drops first (they're "wrote anyway" — less alarming), then
            # the hard kills sorted by count desc so the biggest killer shows first.
            hard_stages = sorted(
                [s for s in drops_by_stage if s != "aliexpress_soft"],
                key=lambda s: len(drops_by_stage[s]),
                reverse=True,
            )
            for stage in hard_stages + (["aliexpress_soft"] if "aliexpress_soft" in drops_by_stage else []):
                examples = drops_by_stage[stage][:10]
                soft = stage == "aliexpress_soft"
                label = f"**{stage}** — {len(drops_by_stage[stage])}"
                if soft:
                    label += " _(soft — written anyway)_"
                st.markdown(label)
                ex_df = _pd.DataFrame([
                    {"Keyword": d.get("keyword", ""), "Reason": d.get("reason", "")}
                    for d in examples
                ])
                st.dataframe(ex_df, hide_index=True, use_container_width=True)
            st.caption(
                "Full drop history is persisted to the **Research Drops** "
                "tab in the Google Sheet."
            )


# ---------------------------------------------------------------------------
# Light table for archive / already-sent (read-only with single action)
# ---------------------------------------------------------------------------

def _render_light_table(keywords, action_label, key_prefix, on_action=None):
    """Compact read-only table used for the two collapsed sections."""
    rows = []
    for k in keywords:
        rows.append({
            "Keyword": k.keyword,
            "Country": k.country,
            "Volume": int(k.monthly_search_volume or 0),
            "Ali €": f"€{k.aliexpress_price:.2f}" if k.aliexpress_price else "—",
            "Comp €": f"€{k.median_competitor_price:.2f}" if k.median_competitor_price else "—",
            "Created": k.created_at[:10],
            "_id": k.keyword_id,
        })
    df = pd.DataFrame(rows)
    st.dataframe(df.drop(columns=["_id"]), hide_index=True, use_container_width=True)

    if action_label and on_action:
        picked = st.selectbox(
            f"Row to {action_label.lower()}",
            options=[""] + [k.keyword_id for k in keywords],
            format_func=lambda kid: "— choose —" if not kid else next(
                (k.keyword for k in keywords if k.keyword_id == kid), kid
            ),
            key=f"{key_prefix}_pick",
        )
        if picked:
            if st.button(action_label, key=f"{key_prefix}_btn"):
                on_action(picked)
                st.success(f"{action_label.rstrip('e')}d.")
                st.rerun()


# ---------------------------------------------------------------------------
# Drops panel — "why did these fail?"
# ---------------------------------------------------------------------------
# Backed by the `Research Drops` Sheets tab, which accumulates one row per
# keyword the pipeline kills (stage + reason). Collapsed by default so it
# doesn't push the active inbox down the page; the ticker in the label tells
# the user at a glance how much has been rejected.
#
# Filtering priorities:
#   - Country scopes to whatever the top-of-page country selector has set.
#     "All" returns everything.
#   - Stage filter lets the user triage by filter type — the common flow is
#     "show me everything dropped at `volume_no_data` so I can see what the
#     LLM is producing that Google has no data for."

# Human-facing labels for each pipeline stage. Ordering matters: the
# selectbox renders in the order declared, and we want the pre-paid (free)
# layers at the top so they're the obvious first target for prompt tuning.
_DROP_STAGE_LABELS: dict[str, str] = {
    "dedup":            "Duplicate — already in inbox / drops",
    "length":           "Too many words (max_keyword_words)",
    "llm_price":        "Layer 1 — outside price band (LLM-self-flagged)",
    "llm_quality":      "Layer 2 — high competition / hard sourcing / not dropshippable",
    "llm_qa":           "Layer 3 — Claude batched QA (below top-N)",
    "volume_no_data":   "No Planner data (too long-tail for Google)",
    "volume":           "Below min monthly search volume",
    "cpc":              "Estimated CPC above cap",
    "competition":      "Failed SerpAPI competition filter",
    "price":            "Outside selling-price band",
    "aliexpress":       "No AliExpress match found",
    "aliexpress_soft":  "AliExpress soft-warn (kept but flagged)",
}


def _render_drops_panel(store, country_filter: str):
    """Persistent view of keywords the discover pipeline has rejected.
    Reads one Sheets page-worth at most (cap=limit). Fails gracefully with
    a caption rather than an error — a missing Research Drops tab happens
    on fresh installs before the first run has written anything."""
    try:
        records = store.get_drop_records(
            country=None if country_filter == "All" else country_filter,
            limit=2000,
        )
    except Exception as e:
        with st.expander("❌ Rejected keywords — error loading"):
            st.caption(f"Couldn't load drop records: {e}")
        return

    label = f"❌ Rejected keywords ({len(records)})"
    with st.expander(label):
        if not records:
            st.caption(
                "No drops recorded yet. Drops accumulate every time the "
                "discover pipeline kills a keyword — they're used both for "
                "transparency here and as an exclusion list so the same "
                "keyword isn't re-researched on future runs."
            )
            return

        # Count per stage for a quick-hit summary + the stage selector.
        by_stage: dict[str, int] = {}
        for r in records:
            by_stage[r.get("stage", "?")] = by_stage.get(r.get("stage", "?"), 0) + 1

        st.caption(
            "Every keyword the pipeline has rejected, with the stage that "
            "killed it. These are also used to dedupe future discovery runs "
            "— the LLM won't waste spend re-ideating candidates we've "
            "already proven don't work."
        )

        # Stage filter — default "All stages" shows everything.
        stage_options = ["All stages"] + [
            f"{_DROP_STAGE_LABELS.get(s, s)}  ({n})"
            for s, n in sorted(by_stage.items(), key=lambda kv: -kv[1])
        ]
        pick = st.selectbox(
            "Filter by stage",
            options=stage_options,
            key="drops_stage_filter",
        )

        filtered = records
        if pick != "All stages":
            # Reverse-lookup the stage key from the selected label by
            # stripping "  (N)" and matching against _DROP_STAGE_LABELS
            # values.
            label_only = pick.rsplit("  (", 1)[0]
            stage_key = next(
                (k for k, v in _DROP_STAGE_LABELS.items() if v == label_only),
                None,
            )
            if stage_key:
                filtered = [r for r in records if r.get("stage") == stage_key]

        # Render table. Newest-first — `get_drop_records` already returns
        # in that order.
        df = pd.DataFrame(
            {
                "When": [(r.get("timestamp") or "")[:16].replace("T", " ") for r in filtered],
                "Country": [r.get("country", "") for r in filtered],
                "Keyword": [r.get("keyword", "") for r in filtered],
                "Stage": [
                    _DROP_STAGE_LABELS.get(r.get("stage", ""), r.get("stage", ""))
                    for r in filtered
                ],
                "Reason": [r.get("reason", "") for r in filtered],
            }
        )
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            height=min(600, 40 + 35 * len(df)),
        )
        st.caption(
            f"Showing {len(filtered)} of {len(records)} drop records. Full "
            "history lives on the **Research Drops** sheet tab."
        )


main()
