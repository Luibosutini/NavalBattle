#!/usr/bin/env bash
# Naval Battle GUI Startup Script (Linux/macOS)
# ブラウザ GUI サーバを起動し、ブラウザを自動で開きます。
# 環境変数はリポジトリ直下の .env（GUI の「初期設定」から生成可能）を読み込みます。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .env ]]; then
  echo "Loading .env"
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "Starting GUI server (http://127.0.0.1:8800/) ..."
exec python -m naval gui "$@"
