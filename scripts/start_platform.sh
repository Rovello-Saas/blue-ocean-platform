#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if command -v npm >/dev/null 2>&1; then
  cd "$ROOT_DIR/internal-page-cloner"
  export npm_config_cache="$ROOT_DIR/.cache/npm"
  export PUPPETEER_CACHE_DIR="$ROOT_DIR/.cache/puppeteer"
  npm install --omit=dev
  npx puppeteer browsers install chrome
  printf 'ok\n' > .install-complete
  printf 'ok\n' > .chrome-install-complete
fi

cd "$ROOT_DIR"
exec streamlit run dashboard/app.py --server.port "${PORT:-8501}" --server.address 0.0.0.0 --server.headless true
