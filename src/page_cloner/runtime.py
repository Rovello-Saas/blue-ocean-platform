"""
Internal page-cloner runtime manager.

The Streamlit dashboard is the product UI. The JavaScript cloner code lives in
this repo under `internal-page-cloner/` and is started on localhost when the
Clone workflow needs it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional, Sequence, TextIO
from urllib.parse import urlparse

import requests

from src.core.config import PAGE_CLONER_URL, PROJECT_ROOT


_PROCESS: Optional[subprocess.Popen] = None
_PROCESS_LOG: Optional[TextIO] = None
_INSTALL_DONE = False
_CHROME_INSTALL_DONE = False
_SETUP_OUTPUT_LIMIT = 4000
_BROWSER_ENV_KEYS = (
    "PUPPETEER_EXECUTABLE_PATH",
    "CHROME_PATH",
    "GOOGLE_CHROME_BIN",
    "CHROMIUM_PATH",
)
_BROWSER_APP_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
)
_BROWSER_BIN_CANDIDATES = (
    "google-chrome-stable",
    "google-chrome",
    "chromium-browser",
    "chromium",
    "microsoft-edge-stable",
    "microsoft-edge",
)


def _is_local_url(url: str) -> bool:
    host = urlparse(url).hostname
    return host in {"localhost", "127.0.0.1", "::1"}


def _health_ok(base_url: str) -> bool:
    try:
        response = requests.get(f"{base_url.rstrip('/')}/api/health", timeout=2)
        if not response.ok:
            return False
        data = response.json()
        return data.get("service") == "page-cloner"
    except Exception:
        return False


def _format_setup_output(output: str | bytes | None) -> str:
    if not output:
        return ""
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    output = output.strip()
    if len(output) > _SETUP_OUTPUT_LIMIT:
        output = output[-_SETUP_OUTPUT_LIMIT:]
    return f"\n\nLast setup output:\n{output}"


def _tail_file(path: Path) -> str:
    try:
        content = path.read_text(errors="replace").strip()
    except OSError:
        return ""
    if len(content) > _SETUP_OUTPUT_LIMIT:
        content = content[-_SETUP_OUTPUT_LIMIT:]
    return f"\n\nLast page-cloner output:\n{content}" if content else ""


def _run_setup_command(
    command: Sequence[str],
    *,
    cwd: Path,
    env: dict,
    timeout: int,
    failure_message: str,
) -> None:
    try:
        subprocess.run(
            list(command),
            cwd=cwd,
            env=env,
            check=True,
            timeout=timeout,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{failure_message} The command timed out after {timeout} seconds."
            f"{_format_setup_output(exc.stdout)}"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"{failure_message} The command exited with status {exc.returncode}."
            f"{_format_setup_output(exc.stdout)}"
        ) from exc


def _safe_remove_tree(path: Path) -> None:
    """Best-effort recursive removal that tolerates Streamlit overlay quirks."""
    if not path.exists():
        return
    try:
        shutil.rmtree(path)
        return
    except OSError:
        pass

    subprocess.run(["find", str(path), "-mindepth", "1", "-delete"], check=False)
    try:
        path.rmdir()
    except OSError:
        pass


def _is_usable_browser_path(path: str | Path | None) -> bool:
    if not path:
        return False
    try:
        return Path(path).exists()
    except (OSError, TypeError):
        return False


def _find_system_chrome() -> Optional[Path]:
    for env_key in _BROWSER_ENV_KEYS:
        candidate = os.environ.get(env_key)
        if _is_usable_browser_path(candidate):
            return Path(candidate)

    for candidate in _BROWSER_APP_CANDIDATES:
        if _is_usable_browser_path(candidate):
            return Path(candidate)

    for binary in _BROWSER_BIN_CANDIDATES:
        candidate = shutil.which(binary)
        if candidate and _is_usable_browser_path(candidate):
            return Path(candidate)

    return None


def _env_browser_path(env: dict) -> Optional[Path]:
    for env_key in _BROWSER_ENV_KEYS:
        candidate = env.get(env_key)
        if _is_usable_browser_path(candidate):
            return Path(candidate)
    return None


def _chrome_marker_is_current(chrome_marker: Path, chrome_cache: Path) -> bool:
    if not chrome_marker.exists():
        return False
    try:
        marker = chrome_marker.read_text().strip()
    except OSError:
        return False
    if marker.startswith("system:"):
        return _is_usable_browser_path(marker.removeprefix("system:"))
    return marker == "ok" and chrome_cache.exists()


def _is_browser_install_cache_error(error: RuntimeError) -> bool:
    message = str(error).lower()
    return any(
        phrase in message
        for phrase in (
            "end of central directory",
            "all providers failed",
            "zip",
            "corrupt",
        )
    )


def _move_aside_node_modules(node_modules: Path) -> None:
    """
    Remove node_modules without asking npm ci to clean it.

    On Streamlit Cloud an interrupted install can leave directories that fail
    npm's internal cleanup with ENOTEMPTY. Renaming the old tree first gives
    npm a clean destination even if deleting the old tree is slow or flaky.
    """
    if not node_modules.exists():
        return

    trash = node_modules.with_name(f".node_modules-trash-{int(time.time())}")
    try:
        node_modules.rename(trash)
        _safe_remove_tree(trash)
        return
    except OSError:
        _safe_remove_tree(node_modules)


def _marker_is_current(install_marker: Path, node_modules: Path, lockfile: Path) -> bool:
    if not install_marker.exists() or not node_modules.exists():
        return False
    try:
        marker = install_marker.read_text().strip()
    except OSError:
        return False
    if not marker:
        return False
    try:
        return float(marker) >= lockfile.stat().st_mtime
    except (OSError, ValueError):
        return False


def _ensure_node_modules(cloner_dir: Path) -> None:
    global _INSTALL_DONE
    install_marker = cloner_dir / ".install-complete"
    node_modules = cloner_dir / "node_modules"
    lockfile = cloner_dir / "package-lock.json"
    if _INSTALL_DONE or _marker_is_current(install_marker, node_modules, lockfile):
        _INSTALL_DONE = True
        return

    if not shutil.which("npm"):
        raise RuntimeError("npm is not available, so the built-in page cloner cannot install dependencies.")

    # Wipe any partial node_modules before running npm ci. On Streamlit
    # Cloud's overlay filesystem, an interrupted previous install can leave
    # orphaned files that npm ci's own cleanup hits ENOTEMPTY on (most often
    # in puppeteer-core/lib/esm/puppeteer/bidi). shutil.rmtree + find -delete
    # are more reliable than letting npm clean itself up, so we always start
    # from a known-clean state.
    _move_aside_node_modules(node_modules)

    install_env = os.environ.copy()
    install_env.setdefault("npm_config_cache", str(PROJECT_ROOT / ".cache" / "npm"))
    install_env.setdefault("PUPPETEER_SKIP_DOWNLOAD", "true")

    command = ["npm", "ci", "--omit=dev", "--no-audit", "--no-fund"]
    try:
        _run_setup_command(
            command,
            cwd=cloner_dir,
            env=install_env,
            timeout=900,
            failure_message="The built-in page cloner could not install Node dependencies.",
        )
    except RuntimeError as exc:
        # npm can still fail with ENOTEMPTY when it races an old partial tree.
        # Clean once more and retry from a known-empty destination before
        # surfacing the error to Streamlit.
        if "ENOTEMPTY" not in str(exc) and "directory not empty" not in str(exc).lower():
            raise
        _move_aside_node_modules(node_modules)
        _run_setup_command(
            command,
            cwd=cloner_dir,
            env=install_env,
            timeout=900,
            failure_message="The built-in page cloner could not install Node dependencies after cleaning the previous install.",
        )

    install_marker.write_text(f"{lockfile.stat().st_mtime}\n")
    _INSTALL_DONE = True


# Secrets the Node cloner reads via process.env. On Streamlit Cloud these are
# set through the Secrets panel and surfaced as st.secrets, NOT os.environ —
# so the Node subprocess can't see them unless we bridge them in here. Without
# this, the cloner silently falls back to "upload originals, no Nano Banana
# edits" and you get untranslated, source-branded images.
_NODE_SECRET_KEYS = (
    "FAL_API_KEY",
    "ANTHROPIC_API_KEY",
    "GOOGLE_GEMINI_API_KEY",
    "MOVANELLA_SHOPIFY_ACCESS_TOKEN",
    "MERIVALO_SHOPIFY_ACCESS_TOKEN",
)


def _bridge_streamlit_secrets(env: dict) -> None:
    """Copy known API keys from st.secrets into the env dict if missing."""
    try:
        import streamlit as st
    except Exception:
        return
    for key in _NODE_SECRET_KEYS:
        if env.get(key):
            continue
        try:
            value = st.secrets.get(key)
        except Exception:
            value = None
        if value:
            env[key] = str(value)


def _runtime_env(base_url: str) -> dict:
    parsed = urlparse(base_url)
    port = str(parsed.port or 3000)
    env = os.environ.copy()
    _bridge_streamlit_secrets(env)
    env.setdefault("HOST", "127.0.0.1")
    env.setdefault("PORT", port)
    env.setdefault("PLATFORM_API_URL", "http://127.0.0.1:8501")
    env.setdefault("PUPPETEER_CACHE_DIR", str(PROJECT_ROOT / ".cache" / "puppeteer"))
    env.setdefault("npm_config_cache", str(PROJECT_ROOT / ".cache" / "npm"))
    if not env.get("PUPPETEER_EXECUTABLE_PATH"):
        system_chrome = _find_system_chrome()
        if system_chrome:
            env["PUPPETEER_EXECUTABLE_PATH"] = str(system_chrome)
    return env


def _ensure_chrome(cloner_dir: Path, env: dict) -> None:
    global _CHROME_INSTALL_DONE
    chrome_marker = cloner_dir / ".chrome-install-complete"
    chrome_cache = Path(env["PUPPETEER_CACHE_DIR"])
    if _CHROME_INSTALL_DONE or _chrome_marker_is_current(chrome_marker, chrome_cache):
        _CHROME_INSTALL_DONE = True
        return

    system_chrome = _env_browser_path(env) or _find_system_chrome()
    if system_chrome:
        env["PUPPETEER_EXECUTABLE_PATH"] = str(system_chrome)
        chrome_marker.write_text(f"system:{system_chrome}\n")
        _CHROME_INSTALL_DONE = True
        return

    if not shutil.which("npx"):
        raise RuntimeError("npx is not available, so the built-in page cloner cannot install Chrome.")

    try:
        _run_setup_command(
            ["npx", "puppeteer", "browsers", "install", "chrome"],
            cwd=cloner_dir,
            env=env,
            timeout=300,
            failure_message="The built-in page cloner could not install Chrome for Puppeteer.",
        )
    except RuntimeError as exc:
        if not _is_browser_install_cache_error(exc):
            raise
        _safe_remove_tree(chrome_cache)
        _run_setup_command(
            ["npx", "puppeteer", "browsers", "install", "chrome@stable"],
            cwd=cloner_dir,
            env=env,
            timeout=300,
            failure_message="The built-in page cloner could not install Chrome for Puppeteer after cleaning the browser cache.",
        )
    chrome_marker.write_text("ok\n")
    _CHROME_INSTALL_DONE = True


def ensure_internal_page_cloner() -> str:
    """
    Start the bundled cloner if PAGE_CLONER_URL points at localhost.

    Returns the base URL Streamlit should call. If PAGE_CLONER_URL is set to a
    non-local URL, we leave it alone so staging/legacy deployments can still
    point to a separate service deliberately.
    """
    global _PROCESS, _PROCESS_LOG

    base_url = PAGE_CLONER_URL.rstrip("/")
    if not _is_local_url(base_url):
        return base_url

    cloner_dir = PROJECT_ROOT / "internal-page-cloner"
    if not cloner_dir.exists():
        raise RuntimeError(f"Built-in page cloner folder is missing: {cloner_dir}")

    env = _runtime_env(base_url)

    if _health_ok(base_url):
        return base_url

    _ensure_node_modules(cloner_dir)
    _ensure_chrome(cloner_dir, env)

    if _health_ok(base_url):
        return base_url

    if _PROCESS and _PROCESS.poll() is None:
        return base_url

    node_bin = shutil.which("node")
    if not node_bin:
        raise RuntimeError("node is not available, so the built-in page cloner cannot start.")

    log_path = PROJECT_ROOT / ".cache" / "page-cloner" / "runtime.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if _PROCESS_LOG:
        try:
            _PROCESS_LOG.close()
        except OSError:
            pass
    _PROCESS_LOG = log_path.open("a", encoding="utf-8")
    _PROCESS_LOG.write(f"\n--- starting page cloner on {base_url} ---\n")
    _PROCESS_LOG.flush()

    _PROCESS = subprocess.Popen(
        [node_bin, "server.js"],
        cwd=cloner_dir,
        env=env,
        stdout=_PROCESS_LOG,
        stderr=subprocess.STDOUT,
    )

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if _PROCESS.poll() is not None:
            raise RuntimeError(
                "Built-in page cloner stopped during startup."
                f"{_tail_file(log_path)}"
            )
        if _health_ok(base_url):
            return base_url
        time.sleep(0.5)

    raise RuntimeError(
        "Built-in page cloner did not become ready in time."
        f"{_tail_file(log_path)}"
    )
