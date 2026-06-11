# Naval Battle — Architecture

最終更新: 2026-06-11（ブラウザ GUI 追加に伴い作成）

## 全体構成

```
                        ┌────────────────────────────┐
  ┌──────────┐  HTTP/SSE │ Web GUI server             │
  │ ブラウザ  │◄────────►│ naval/web/server.py        │
  │ (静的JS)  │          │ (FastAPI, port 8800)       │
  └──────────┘          └─────────────┬──────────────┘
                                      │
  ┌──────────┐                        ▼
  │ CLI       │──────────► ┌────────────────────────────┐
  │ naval/cli │            │ サービス層                  │
  └──────────┘  表示のみ    │ naval/service.py            │
       │                   │ (FleetService: 構造化データ) │
       ▼                   └─────────────┬──────────────┘
  ┌────────────────┐                     │ boto3
  │ ランタイム層     │                     ▼
  │ naval/runtime/ │        ┌────────────────────────────┐
  │  aws_runtime   │        │ AWS                        │
  │  temporal_...  │        │  SQS (fleet-missions)      │
  └────────────────┘        │  DynamoDB (mission/task/   │
                            │            budget state)   │
  ┌────────────────┐        │  S3 (tickets/missions/     │
  │ ワーカー        │◄──────│      artifacts/comms)      │
  │ fleet_node.py  │        │  Bedrock (モデル呼び出し)    │
  └────────────────┘        └────────────────────────────┘
```

## レイヤー責務

### サービス層 — `naval/service.py`

CLI と Web GUI が共有するビジネスロジック。すべての操作結果を
dataclass（`MissionSummary` / `MissionDetail` / `TaskDetail` / `StageStatus` /
`BudgetStatus` / `ArtifactFile` / `CommEntry`）で返し、表示は一切行わない。

- ミッション: 一覧 / 承認・入力待ち一覧 / 詳細（通信ログ込み）
- HITL シグナル: `approve_mission` / `send_input` / `resume_mission`
  （DynamoDB の条件付き更新 + SQS 再投入）
- タスク: 詳細（ステージ・確信度） / 中断 / リトライ / CA 指令保存 /
  一括承認・入力 / `task_orchestrator.py` 経由の投入（init/start/advance）
- 予算・成果物（S3）・チケットアップロード
- エラーは `FleetServiceError` 系例外（メッセージはユーザー表示可能）
- boto3 クライアントは注入可能（ユニットテストはフェイクで実施）

### ランタイム層 — `naval/runtime/`

`RuntimeBase` 抽象クラスに対し AWS / Temporal の 2 実装。
`AwsRuntime` は `FleetService` へ委譲する表示層で、typer / rich の出力と
対話プロンプトのみを担当する。

### Web 層 — `naval/web/`

| ファイル | 責務 |
|----------|------|
| `server.py` | FastAPI アプリ。`FleetService` を HTTP API 化。SSE `/api/events` はサーバ側ポーリング（`NAVAL_GUI_POLL_SEC`、既定 3 秒）で差分のみ配信。承認・入力待ちの監視スレッドが OS / Slack 通知を送る |
| `ships_config.py` | `config.yaml` の `ships:`（艦種ごとの AI エージェント設定）の検証付き読み書き。バックアップ→round-trip 検証→書き込み→`fleet_local` キャッシュ無効化の順で安全に保存 |
| `static/` | ビルド工程なしの素の HTML/JS/CSS（日本語 UI） |

主要 API:

| メソッド | パス | 内容 |
|----------|------|------|
| GET | `/api/missions` / `/api/pending` | ミッション一覧 / 承認・入力待ち |
| GET/POST | `/api/missions/{id}` 系 | 詳細 / approve / input / artifacts |
| POST | `/api/missions` | 新規投入（ticket→S3→orchestrator init/start） |
| GET/POST | `/api/tasks/{id}` 系 | 詳細 / abort / retry / ca / approve / input |
| GET | `/api/budget` / `/api/doctor` / `/api/config` | 予算 / 環境チェック / 設定値 |
| GET/PUT | `/api/fleet/ships[/{ship}]` | 艦種エージェント設定 |
| GET | `/api/fleet/models` / `/api/fleet/formations` | モデル候補 / 陣形 |
| GET/POST | `/api/setup` | `.env` 生成ウィザード |
| GET (SSE) | `/api/events` | ミッション一覧のライブ更新 |

## 艦種ごとの AI エージェント設定

設定は 2 層で構成される:

1. **`config.yaml` の `ships:`** — 艦種（CVL/DD/CL/CVB/CA/BB と `DD_SONNET` 等の
   派生）ごとのプロファイル。`model_env`（使用する環境変数名）、`model_tag`、
   `armament`（`max_tokens` / `temperature`）、ペルソナ（`role` / `voice` /
   `objective` / `thinking` / `constraints` / `checklist` / `focus`）
2. **環境変数 `BEDROCK_*_MODEL_ID`** — 実際の Bedrock inference profile ARN

GUI の「艦隊設定」は 1 を編集し（`model_env` の切替＝担当モデル変更）、
「初期設定」フォームが 2 を `.env` に書き出す。ワーカー（`fleet_node.py`）は
環境変数を起動時に読むため、モデル割当・armament の変更は**ワーカー再起動後に
反映**される（GUI 上にもその旨を表示する）。

## データストア（変更なし）

| ストア | 用途 |
|--------|------|
| DynamoDB `fleet-mission-state` | ミッション状態（status / needs_* / confidence_level / human_response） |
| DynamoDB `fleet-task-state` | タスク状態（current_stage / stages マップ） |
| DynamoDB `fleet-budget` | 月次予算（fuel / ammo） |
| S3 | `tickets/` `missions/{id}/orders|artifacts|comms|results/` |
| SQS `fleet-missions` | ミッションキュー（HITL 再開時に再投入） |

## 起動方法

| 方法 | コマンド |
|------|----------|
| CLI から | `python -m naval gui`（ブラウザ自動オープン、`--no-browser` 可） |
| スクリプト | `scripts/start_gui.sh` / `scripts/start_gui.ps1`（`.env` 自動読込） |
| Docker | `docker compose -f docker-compose.naval-local.yml up naval-gui` |

## セキュリティ上の前提

- GUI / API に認証はない。`127.0.0.1`（既定）または信頼できる LAN 内でのみ
  使用すること。インターネットへ公開しない。
