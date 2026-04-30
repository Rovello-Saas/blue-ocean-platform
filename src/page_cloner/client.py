"""
HTTP client for the Node page-cloner service.

The server exposes an async job API:
    POST /api/jobs          start a clone, returns {jobId, status, ...}
    GET  /api/jobs/:id      full job state with status/progress/steps/result/error
    GET  /api/jobs          list all jobs (most recent first)

A clone takes minutes (scrape → AI generate → upload N images → push template
→ publish → reviews), so this client uses the start-and-poll pattern rather
than a long blocking call. Callers normally just use `clone()` which handles
both sides and returns the final `result` dict on success.

Failure model:
    PageClonerUnavailable  — can't reach the server at all (it's not running,
                             wrong URL, network blocked). Caller should check
                             whether the Node service is up.
    PageClonerJobFailed    — server accepted the job but the pipeline errored.
                             Wraps the error message from the job record.
    PageClonerTimeout      — polling exceeded the timeout. The job may still
                             finish eventually; caller can re-poll with `get_job()`.
    PageClonerError        — base class. Also raised directly for unexpected
                             HTTP statuses (5xx, malformed responses).

The client never raises on 404 for `get_job` — callers that want to discover
unknown job IDs should handle the `PageClonerError` explicitly.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

from src.core.config import PAGE_CLONER_URL
from src.page_cloner.runtime import ensure_internal_page_cloner

logger = logging.getLogger(__name__)


# Default poll cadence. A typical Merivalo clone takes 2–5 minutes, so 3s
# polling means ~40–100 HTTP calls per clone — negligible against a localhost
# Node server. The timeout covers the slowest runs (many images + translation).
DEFAULT_POLL_INTERVAL_SEC = 3.0
DEFAULT_TIMEOUT_SEC = 900  # 15 minutes

# Statuses that mean "keep polling". Anything else → terminal.
# Derived from the page-cloner's step names (see src/routes/api.js).
_ACTIVE_STATUSES = {
    "scraping",
    "generating",
    "creating",
    "translating",
    "pushing",
    "publishing",
}
_SUCCESS_STATUS = "done"
_FAILURE_STATUS = "failed"


class PageClonerError(Exception):
    """Base class for page-cloner client errors."""


class PageClonerUnavailable(PageClonerError):
    """The page-cloner server can't be reached (connection refused, DNS, etc.)."""


class PageClonerJobFailed(PageClonerError):
    """The server ran the job but the pipeline errored. `.job` has the full record."""

    def __init__(self, message: str, job: dict):
        super().__init__(message)
        self.job = job


class PageClonerTimeout(PageClonerError):
    """Polling exceeded the timeout. `.job_id` lets the caller resume polling."""

    def __init__(self, message: str, job_id: str):
        super().__init__(message)
        self.job_id = job_id


class PageClonerClient:
    """
    Thin HTTP wrapper around the Node page-cloner.

    Single responsibility: start jobs and observe them. Interpretation of the
    result (what to write into Sheets, how to link products to keywords) lives
    in the caller, not here.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        request_timeout_sec: float = 10.0,
    ):
        # `request_timeout_sec` is the per-request HTTP timeout, distinct from
        # the overall polling timeout. Start/poll calls should return fast;
        # anything slower than this on localhost is a symptom, not normal.
        self.base_url = (base_url or ensure_internal_page_cloner()).rstrip("/")
        self.request_timeout_sec = request_timeout_sec

    # ------------------------------------------------------------------ health

    def health_check(self) -> bool:
        """
        Cheap reachability probe. Prefer `/api/health` when the service exposes
        it, and fall back to `/api/jobs` for older page-cloner deployments. Does
        not raise — useful for dashboards that want to show "page cloner: up /
        down" without try/except noise.
        """
        try:
            r = requests.get(
                f"{self.base_url}/api/health",
                timeout=self.request_timeout_sec,
            )
            if r.ok:
                data = r.json()
                return data.get("service") == "page-cloner"

            r = requests.get(
                f"{self.base_url}/api/jobs",
                timeout=self.request_timeout_sec,
            )
            if not r.ok:
                return False
            return isinstance(r.json(), list)
        except (ValueError, requests.RequestException):
            return False

    # ------------------------------------------------------------------- start

    def start_clone(
        self,
        source_url: str,
        store: str,
        target_language: Optional[str] = None,
    ) -> str:
        """
        Kick off a clone job. Returns the server-assigned job ID immediately;
        the pipeline runs asynchronously on the Node side.

        Args:
            source_url:      Competitor product page to clone.
            store:           Store ID, e.g. "movanella" or "merivalo". Must match
                             a file in the Node side's stores/ directory.
            target_language: ISO code like "de" to translate title/body/images,
                             or None to use the store's default language.

        Raises:
            PageClonerUnavailable  Node service unreachable.
            PageClonerError        Server returned 4xx/5xx.
        """
        payload = {"url": source_url, "storeId": store}
        if target_language:
            payload["targetLanguage"] = target_language

        try:
            r = requests.post(
                f"{self.base_url}/api/jobs",
                json=payload,
                timeout=self.request_timeout_sec,
            )
        except requests.ConnectionError as e:
            raise PageClonerUnavailable(
                f"Cannot reach page cloner at {self.base_url}: {e}"
            ) from e
        except requests.RequestException as e:
            raise PageClonerError(f"HTTP error starting job: {e}") from e

        if not r.ok:
            raise PageClonerError(
                f"Page cloner rejected job (HTTP {r.status_code}): {r.text[:200]}"
            )

        data = r.json()
        job_id = data.get("jobId")
        if not job_id:
            raise PageClonerError(f"Page cloner response missing jobId: {data}")

        logger.info(
            "Started page-cloner job %s (url=%s, store=%s, lang=%s)",
            job_id,
            source_url,
            store,
            target_language or "default",
        )
        return job_id

    # -------------------------------------------------------------------- poll

    def get_job(self, job_id: str) -> dict:
        """
        Fetch the current state of a job. Returns the full record (status,
        progress, steps, result, error, ...).

        Raises PageClonerError on HTTP failures including 404 — unknown job IDs
        are treated as a caller bug, not a silent miss.
        """
        try:
            r = requests.get(
                f"{self.base_url}/api/jobs/{job_id}",
                timeout=self.request_timeout_sec,
            )
        except requests.ConnectionError as e:
            raise PageClonerUnavailable(
                f"Cannot reach page cloner at {self.base_url}: {e}"
            ) from e
        except requests.RequestException as e:
            raise PageClonerError(f"HTTP error polling job {job_id}: {e}") from e

        if not r.ok:
            raise PageClonerError(
                f"Page cloner get_job {job_id} returned HTTP {r.status_code}: {r.text[:200]}"
            )

        return r.json()

    # ---------------------------------------------------------------- convenience

    def clone(
        self,
        source_url: str,
        store: str,
        target_language: Optional[str] = None,
        poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        on_progress: Optional[callable] = None,
    ) -> dict:
        """
        Start a clone and block until it finishes. Returns the `result` dict
        from the completed job (productUrl, adminUrl, handle, productMeta,
        reviews, qa, ...).

        `on_progress(job)` is called with the full job record after each poll,
        so a UI can render step-by-step status. Any exception from that
        callback is swallowed and logged — progress reporting must never
        break the clone.

        Raises:
            PageClonerUnavailable  Service unreachable.
            PageClonerJobFailed    Pipeline ran but errored; `.job` has details.
            PageClonerTimeout      Didn't finish within `timeout_sec`.
            PageClonerError        Unexpected HTTP/response issues.
        """
        job_id = self.start_clone(source_url, store, target_language)

        deadline = time.monotonic() + timeout_sec
        last_status: Optional[str] = None

        while True:
            job = self.get_job(job_id)
            status = job.get("status", "unknown")

            # Fire progress callback even on unchanged status so the caller
            # gets regular heartbeats (useful for "still running…" UIs).
            if on_progress is not None:
                try:
                    on_progress(job)
                except Exception as cb_err:  # noqa: BLE001 — callback isolation
                    logger.warning(
                        "on_progress callback raised (ignored): %s", cb_err
                    )

            if status != last_status:
                logger.debug(
                    "Job %s: %s (progress=%s)",
                    job_id,
                    status,
                    job.get("progress"),
                )
                last_status = status

            if status == _SUCCESS_STATUS:
                result = job.get("result")
                if not result:
                    # Shouldn't happen — server marks `done` only after writing
                    # `result`. Treat as a server bug if it does.
                    raise PageClonerError(
                        f"Job {job_id} status=done but no result payload"
                    )
                return result

            if status == _FAILURE_STATUS:
                err_msg = job.get("error") or "no error message"
                raise PageClonerJobFailed(
                    f"Job {job_id} failed: {err_msg}", job=job
                )

            if status not in _ACTIVE_STATUSES:
                # Defensive: an unknown status shouldn't hang us forever. Log it
                # and keep polling until deadline, so a future status name added
                # on the Node side doesn't break us silently.
                logger.warning(
                    "Job %s in unknown status %r — continuing to poll", job_id, status
                )

            if time.monotonic() >= deadline:
                raise PageClonerTimeout(
                    f"Job {job_id} did not finish within {timeout_sec}s "
                    f"(last status: {status})",
                    job_id=job_id,
                )

            time.sleep(poll_interval_sec)
