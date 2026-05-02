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


def _ensure_node_modules(cloner_dir: Path) -> None:
    global _INSTALL_DONE
    install_marker = cloner_dir / ".install-complete"
    node_modules = cloner_dir / "node_modules"
    if _INSTALL_DONE or (install_marker.exists() and node_modules.exists()):
        _INSTALL_DONE = True
        return

    if not shutil.which("npm"):
        raise RuntimeError("npm is not available, so the built-in page cloner cannot install dependencies.")

    install_env = os.environ.copy()
    install_env.setdefault("npm_config_cache", str(PROJECT_ROOT / ".cache" / "npm"))
    install_env.setdefault("PUPPETEER_SKIP_DOWNLOAD", "true")

    _run_setup_command(
        ["npm", "ci", "--omit=dev", "--no-audit", "--no-fund"],
        cwd=cloner_dir,
        env=install_env,
        timeout=900,
        failure_message="The built-in page cloner could not install Node dependencies.",
    )
    install_marker.write_text("ok\n")
    _INSTALL_DONE = True


def _runtime_env(base_url: str) -> dict:
    parsed = urlparse(base_url)
    port = str(parsed.port or 3000)
    env = os.environ.copy()
    env.setdefault("HOST", "127.0.0.1")
    env.setdefault("PORT", port)
    env.setdefault("PLATFORM_API_URL", "http://127.0.0.1:8501")
    env.setdefault("PUPPETEER_CACHE_DIR", str(PROJECT_ROOT / ".cache" / "puppeteer"))
    env.setdefault("npm_config_cache", str(PROJECT_ROOT / ".cache" / "npm"))
    return env


def _ensure_chrome(cloner_dir: Path, env: dict) -> None:
    global _CHROME_INSTALL_DONE
    chrome_marker = cloner_dir / ".chrome-install-complete"
    chrome_cache = Path(env["PUPPETEER_CACHE_DIR"])
    if _CHROME_INSTALL_DONE or (chrome_marker.exists() and chrome_cache.exists()):
        _CHROME_INSTALL_DONE = True
        return

    if not shutil.which("npx"):
        raise RuntimeError("npx is not available, so the built-in page cloner cannot install Chrome.")

    _run_setup_command(
        ["npx", "puppeteer", "browsers", "install", "chrome"],
        cwd=cloner_dir,
        env=env,
        timeout=300,
        failure_message="The built-in page cloner could not install Chrome for Puppeteer.",
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
