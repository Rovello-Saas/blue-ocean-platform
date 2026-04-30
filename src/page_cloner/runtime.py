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
from typing import Optional
from urllib.parse import urlparse

import requests

from src.core.config import PAGE_CLONER_URL, PROJECT_ROOT


_PROCESS: Optional[subprocess.Popen] = None
_INSTALL_DONE = False
_CHROME_INSTALL_DONE = False


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


def _ensure_node_modules(cloner_dir: Path) -> None:
    global _INSTALL_DONE
    install_marker = cloner_dir / ".install-complete"
    if _INSTALL_DONE or install_marker.exists():
        _INSTALL_DONE = True
        return

    if not shutil.which("npm"):
        raise RuntimeError("npm is not available, so the built-in page cloner cannot install dependencies.")

    install_env = os.environ.copy()
    install_env.setdefault("npm_config_cache", str(PROJECT_ROOT / ".cache" / "npm"))

    subprocess.run(
        ["npm", "install", "--omit=dev"],
        cwd=cloner_dir,
        env=install_env,
        check=True,
        timeout=240,
    )
    install_marker.write_text("ok\n")
    _INSTALL_DONE = True


def _ensure_chrome(cloner_dir: Path, env: dict) -> None:
    global _CHROME_INSTALL_DONE
    chrome_marker = cloner_dir / ".chrome-install-complete"
    if _CHROME_INSTALL_DONE or chrome_marker.exists():
        _CHROME_INSTALL_DONE = True
        return

    if not shutil.which("npx"):
        raise RuntimeError("npx is not available, so the built-in page cloner cannot install Chrome.")

    subprocess.run(
        ["npx", "puppeteer", "browsers", "install", "chrome"],
        cwd=cloner_dir,
        env=env,
        check=True,
        timeout=300,
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
    global _PROCESS

    base_url = PAGE_CLONER_URL.rstrip("/")
    if not _is_local_url(base_url):
        return base_url

    if _health_ok(base_url):
        return base_url

    cloner_dir = PROJECT_ROOT / "internal-page-cloner"
    if not cloner_dir.exists():
        raise RuntimeError(f"Built-in page cloner folder is missing: {cloner_dir}")

    parsed = urlparse(base_url)
    port = str(parsed.port or 3000)
    env = os.environ.copy()
    env.setdefault("HOST", "127.0.0.1")
    env.setdefault("PORT", port)
    env.setdefault("PLATFORM_API_URL", "http://127.0.0.1:8501")
    env.setdefault("PUPPETEER_CACHE_DIR", str(PROJECT_ROOT / ".cache" / "puppeteer"))

    _ensure_node_modules(cloner_dir)
    _ensure_chrome(cloner_dir, env)

    if _PROCESS and _PROCESS.poll() is None:
        return base_url

    _PROCESS = subprocess.Popen(
        ["node", "server.js"],
        cwd=cloner_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if _PROCESS.poll() is not None:
            raise RuntimeError("Built-in page cloner stopped during startup.")
        if _health_ok(base_url):
            return base_url
        time.sleep(0.5)

    raise RuntimeError("Built-in page cloner did not become ready in time.")
