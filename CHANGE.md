# CHANGE.md — 変更記録

## 2026-06-11: ブラウザ GUI の追加（CUI 非依存化）

計画書: `docs/GUI_IMPROVEMENT_PLAN.ja.md`

CUI 操作ができないユーザーでも扱えるよう、ローカル Web GUI を追加した。
既存 CLI（`python -m naval ...`）の動作は変更していない。

### フェーズ 0: ロジックと I/O の分離

| 変更 | 内容 |
|------|------|
| 追加 `naval/service.py` | AWS ランタイムのビジネスロジックを `FleetService` として抽出。結果を dataclass（`MissionSummary` / `MissionDetail` / `TaskDetail` / `StageStatus` / `BudgetStatus` / `ArtifactFile` / `CommEntry`）で返す。エラーは `FleetServiceError`（`MissionNotFound` / `TaskNotFound` / `TaskAlreadyFinished`）。boto3 クライアントは注入可能 |
| 変更 `naval/runtime/aws_runtime.py` | `FleetService` へ委譲する表示層に書き換え。`typer.echo` / rich 表示は CLI 側に残し、出力メッセージは従来と同一 |
| 追加 `tests/unit/test_fleet_service.py` | フェイク boto3 クライアントによるサービス層のユニットテスト |

### フェーズ 1: REST / SSE API サーバ

| 変更 | 内容 |
|------|------|
| 追加 `naval/web/server.py` | FastAPI アプリ。`/api/missions`（一覧・詳細・投入・承認・入力・成果物）、`/api/tasks`（一覧・詳細・中断・リトライ・CA指示・一括承認/入力）、`/api/pending`、`/api/budget`、`/api/doctor`、`/api/config`、`/api/setup`、`/api/fleet/*`、SSE `/api/events` |
| 追加 `naval/web/ships_config.py` | `config.yaml` の `ships:`（艦種ごとの AI エージェント設定）を検証付きで読み書き。書き込み前バックアップ（`config.yaml.bak`）、`ships:` 以外のセクション・未知キーは保持、保存後に `fleet_local` のプロセス内キャッシュを無効化。`formations:` とモデル候補（`BEDROCK_*_MODEL_ID`）の列挙も提供 |
| 変更 `naval/cli.py` | `naval gui` コマンドを追加（サーバ起動＋ブラウザ自動オープン、`--host/--port/--no-browser`） |
| 変更 `requirements.txt` | `fastapi`, `uvicorn` を追加 |
| 追加 `tests/unit/test_ships_config.py`, `tests/functional/test_web_api.py` | 設定読み書きと API の機能テスト |

### フェーズ 2: ブラウザ GUI

| 変更 | 内容 |
|------|------|
| 追加 `naval/web/static/` (`index.html` / `app.js` / `style.css`) | ビルド不要の素の HTML/JS/CSS。日本語 UI |

画面構成:
- **ダッシュボード**: ミッションカード一覧。SSE でライブ更新、承認・入力待ちは赤枠＋件数バッジ＋画面上部に警告表示
- **新規ミッション**: チケット本文・リポジトリ URL・実行プラン（陣形プルダウン、説明付き）のフォーム投入
- **承認・入力**: 通信ログを確認しながら「承認して再開」「却下」「回答を送信」をボタン操作
- **タスク**: ステージ進行テーブル、中断・リトライ、CA への指示送信、成果物ダウンロード
- **艦隊設定**: 艦種ごとの AI エージェント設定（担当モデルのプルダウン割当、max_tokens / temperature スライダー、役割・口調・目的・思考様式・制約・チェックリストの編集）。モデル・パラメータ変更時はワーカー再起動が必要な旨を表示
- **予算**: 燃料・BB 主砲・CA 斉射・CVB 航空隊の残量ゲージ
- **環境チェック**: `doctor` 相当を ✅/⚠️/❌ で表示、`.env` 生成の初期設定フォーム

### フェーズ 3: 通知とワンクリック起動

| 変更 | 内容 |
|------|------|
| 変更 `naval/notify.py` | Slack Incoming Webhook 通知を追加（`NAVAL_SLACK_WEBHOOK_URL`）。デスクトップ通知と併用 |
| 変更 `naval/web/server.py` | 承認・入力待ちの監視スレッドを追加。新規発生時に OS / Slack 通知（`NAVAL_GUI_NOTIFY=0` で無効化、ブラウザ通知は GUI 側で別途有効化可能） |
| 追加 `scripts/start_gui.ps1` / `scripts/start_gui.sh` | `.env` を自動読込してダブルクリック / ワンコマンドで GUI 起動 |
| 変更 `docker-compose.naval-local.yml` | `naval-gui` サービスを追加（`docker compose up naval-gui` で 8800 番に起動） |

### ドキュメント

| 変更 | 内容 |
|------|------|
| 追加 `CHANGE.md` | 本ファイル |
| 追加 `docs/ARCHITECTURE.md` | Web GUI を含むシステム構成 |
| 変更 `README.md` | GUI の起動方法・`gui` コマンドを追記 |
| 変更 `.gitignore` | `CHANGE.md` / `docs/*.md` を追跡対象に追加 |

### 既知の制約

- GUI（FleetService）は AWS ランタイム向け。Temporal ランタイムの GUI 対応は未実装
- 艦隊設定での「担当モデル」変更は `model_env`（環境変数の割当先）の切替であり、環境変数値そのものの変更は「初期設定」フォームで行う。反映にはワーカー（`fleet_node.py`）の再起動が必要
- 認証なし（localhost / 信頼できる LAN 内での利用前提）
