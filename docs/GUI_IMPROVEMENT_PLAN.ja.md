# NavalBattle GUI 化計画 — CUI が使えない人でも扱えるようにする

- 作成日: 2026-06-11
- ステータス: 計画（未着手）
- 方針決定済み: **ローカル Web GUI**（ブラウザで操作）。当面は 1 人利用、将来的に非エンジニアへの配布を視野に入れる。

---

## 1. 背景と課題

現在、すべての操作が `python -m naval ...` の CLI に依存しており、CUI に不慣れな
ユーザーには実質的に使えない。具体的な障壁は次の 3 点。

| # | 課題 | 該当箇所 |
|---|------|---------|
| 1 | 入力が CLI 引数依存（`--ticket` `--repo-url` `--doctrine` 等を手打ち） | `naval/cli.py` 全コマンド |
| 2 | 出力が `typer.echo` 直結で、ロジックと表示が密結合。結果を整形済みテキストでしか取れない | `naval/runtime/aws_runtime.py`（約 150 箇所）、`temporal_runtime.py` |
| 3 | HITL（承認・入力）が `input()` のブロッキング対話。ターミナルに張り付く必要がある | `aws_runtime.py` の `run()` / `pending()`、`cli.py` の `ca` |

幸い、全操作は `naval/runtime/base.py` の `RuntimeBase` 抽象クラスに集約済みで、
AWS / Temporal の 2 ランタイムがこれを実装している。**この層をそのまま GUI の
バックエンドとして再利用できる**ため、ビジネスロジックの作り直しは不要。

## 2. ゴール

- CUI 操作ができない人が、ブラウザだけでミッション投入〜承認〜結果取得まで完結できる
- ターミナルに張り付かなくても、承認待ち（NEED_APPROVAL / NEED_INPUT）に気づける
- 既存 CLI は壊さず併存させる（上級者・自動化用途）

## 3. 全体アーキテクチャ

```
[ブラウザ GUI] ──HTTP/WebSocket──> [FastAPI サーバ (naval/web/)]
                                        │
                                        ▼
                              [RuntimeBase (既存)]
                              ├── AwsRuntime
                              └── TemporalRuntime
```

## 4. フェーズ別計画

### フェーズ 0: ロジックと I/O の分離（土台・最重要）

現状のランタイムメソッドは「処理して `typer.echo` で印字」する作りのため、
GUI から結果を受け取れない。まずここを直す。

- [ ] `RuntimeBase` の各メソッドが**構造化データ（dataclass）を返す**よう変更する
  - 例: `status()` → `MissionStatus` を返す。`pending()` → `list[PendingMission]` を返す
  - 返却用 dataclass は `naval/artifacts/schema.py` の流儀に合わせて定義
- [ ] 印字処理を CLI 層（`naval/cli.py`）へ移動。CLI の見た目・挙動は変えない
- [ ] ブロッキング対話（`run()` の `Approve? [y/N]`、`pending()` の複数行入力、
      `ca` の対話入力）を「状態の問い合わせ」と「シグナル送信」の 2 つの
      非同期 API に分解する（Temporal ランタイムの approve/input シグナルが既にこの形）
- [ ] 既存ユニットテスト（`tests/unit/`）を維持しつつ、返却データの検証テストを追加

**完了条件:** CLI の全コマンドが従来どおり動き、かつ各操作の結果が Python
オブジェクトとして取得できる。

### フェーズ 1: REST / WebSocket API サーバ

- [ ] `naval/web/server.py` に FastAPI アプリを新設（依存追加: `fastapi`, `uvicorn`）
- [ ] エンドポイント設計:

| メソッド | パス | 対応する既存操作 |
|----------|------|------------------|
| POST | `/api/missions` | `enqueue` / `run` |
| GET | `/api/missions` | `watch` の一覧部分 |
| GET | `/api/missions/{id}` | `status` / `show` |
| POST | `/api/missions/{id}/approve` | `approve --yes/--no` |
| POST | `/api/missions/{id}/input` | `input` |
| POST | `/api/missions/{id}/abort` | `abort` |
| POST | `/api/missions/{id}/retry` | `retry` |
| GET | `/api/missions/{id}/artifacts` | `pull` |
| GET | `/api/pending` | `pending` の一覧部分 |
| GET | `/api/budget` | `status --month` |
| GET | `/api/doctor` | `doctor` |
| GET | `/api/fleet/ships` | `config.yaml` の `ships:` 一覧（艦種ごとのエージェント設定） |
| PUT | `/api/fleet/ships/{ship_class}` | 艦種のエージェント設定を更新（`config.yaml` へ書き戻し） |
| GET | `/api/fleet/models` | 割当可能なモデル一覧（Bedrock inference profile の列挙） |
| WS/SSE | `/api/events` | `watch` のリアルタイム更新 |

- [ ] 艦種設定の読み書きモジュールを追加: `config.yaml` の `ships:` を検証付きで
      更新する（不正な YAML を書き込まない、未知のキーは保持、書き込み前にバックアップ）
- [ ] `--runtime aws|temporal` 相当はサーバ起動時オプションで選択
- [ ] リアルタイム更新は SSE（実装が単純）から始め、必要なら WebSocket 化
- [ ] API の機能テストを `tests/functional/` に追加

**完了条件:** `curl` だけで投入〜承認〜結果取得が一通りできる。

### フェーズ 2: ブラウザ GUI

非エンジニアが触る画面。専門用語を避けたラベルにする（例: doctrine → 「実行プラン」）。

- [ ] **ダッシュボード**: ミッション一覧をカード表示。NEED_APPROVAL / NEED_INPUT を
      赤バッジで最上部に固定表示（現 `watch` TUI の Web 版）
- [ ] **新規ミッション投入フォーム**: チケット本文（テキストエリア）、リポジトリ URL、
      陣形プルダウン、承認ゲート有無のチェックボックス。CLI 引数の知識を不要にする
- [ ] **承認・入力画面**: ミッション詳細＋確信度を表示し、「承認」「却下」ボタンと
      コメント欄。NEED_INPUT には複数行入力ボックス
- [ ] **ミッション詳細**: ステージ進行（`show` 相当）、最近のログ（`tail` 相当）、
      成果物ダウンロードリンク（`pull` 相当）
- [ ] **予算ゲージ**: Fuel / BB / CA / CVB の残量をプログレスバー表示
- [ ] **環境チェック画面**: `doctor` の結果を ✅/⚠️/❌ で表示。初回起動時に自動実行
- [ ] **艦隊設定画面（艦種ごとの AI エージェント設定）**: 艦種（CVL / DD / CL /
      CVB / CA / BB と `DD_SONNET` 等の派生）ごとにカードを並べ、以下を編集可能にする
  - **割当モデル**: 利用可能な Bedrock モデル（inference profile）からプルダウン選択。
    現在の `model_env`（`BEDROCK_*_MODEL_ID`）→ 環境変数という間接参照を GUI が吸収し、
    ユーザーには「この艦種はこのモデル」とだけ見せる
  - **兵装パラメータ**: `max_tokens` / `temperature` をスライダー＋数値入力で編集
  - **ペルソナ・行動指針**: `role`（役割）/ `voice`（口調）/ `objective`（目的）/
    `thinking`（思考様式）/ `constraints` / `checklist` / `focus` をフォーム編集
  - 保存先は既存の `config.yaml` `ships:` セクション（`fleet_local.py` の
    `get_ship_profile()` がそのまま読む形式を維持し、CLI/ワーカーと設定を共有）
  - 編集前に現在値のプレビュー（`ws mental` 相当のメンタルモデル表示）を表示し、
    「保存」「既定値に戻す」を用意

技術選定: ビルド工程を持たない軽量構成（素の HTML + htmx または Alpine.js +
FastAPI の静的配信）を第一候補とする。フロントのビルドチェーンを持ち込まない
ことで、配布と保守を単純に保つ。表現力が足りなくなった時点で SPA 化を検討。

**完了条件:** ターミナルを一度も開かずに、投入→承認→結果ダウンロードが完結する。

### フェーズ 3: 通知とワンクリック起動（張り付き不要化・配布準備）

- [ ] **通知**: NEED_APPROVAL / NEED_INPUT 発生時にブラウザ通知（Web Notifications）。
      既存 `naval/notify.py` を拡張して Slack / メール通知も選択可能にする
- [ ] **ワンコマンド起動**: `python -m naval gui` でサーバ起動＋ブラウザ自動オープン
- [ ] **ダブルクリック起動**: Windows 用 `.ps1` / `start_gui.bat`（既存
      `scripts/start_fleet.ps1` の流儀に合わせる）
- [ ] **Docker 起動**: `docker-compose.naval-local.yml` に GUI サービスを追加し、
      `docker compose up` だけで動く構成
- [ ] **初期設定ウィザード**: 環境変数（リージョン・バケット名等）を GUI 上の
      フォームで入力し `.env` を生成（`naval up` のフォーム版）。
      非エンジニア配布時の最大の障壁である環境構築をここで吸収する

**完了条件:** 非エンジニアに「これをダブルクリックして、ブラウザで操作して」と
渡せる状態。

## 5. スコープ外（今回はやらない）

- 既存 CLI の廃止・変更（併存させる）
- 認証・マルチユーザー対応（ローカル 1 人利用前提。チーム共有が必要になった時点で
  簡易トークン認証を追加）
- インターネット公開（あくまで localhost / LAN 内）

## 6. リスクと対策

| リスク | 対策 |
|--------|------|
| フェーズ 0 のリファクタで既存 CLI を壊す | CLI 出力のスナップショットテストを先に追加してから着手 |
| `watch` / `tail --follow` のポーリングが API 化で複雑になる | まず SSE + サーバ側ポーリングの単純な実装で割り切る |
| 非エンジニアにとって AWS 認証情報の設定が依然難しい | フェーズ 3 の設定ウィザード＋`doctor` 画面でエラーを平易な日本語で説明 |
| 艦種設定を GUI で変更しても反映されない（`fleet_local.py` がプロセス内キャッシュ `_CONFIG_CACHE` を持ち、ワーカーは環境変数を起動時に読む） | 設定保存 API でキャッシュを明示的に無効化。モデル割当の変更時は GUI に「ワーカー再起動が必要」と表示し、将来的にワーカー側のリロード機構を検討 |
| GUI からの `config.yaml` 書き込みで手書き設定（コメント等）を壊す | 書き込み前バックアップ＋`ships:` セクション以外は変更しない方針。検証エラー時は保存を拒否 |

## 7. 推奨着手順

フェーズ 0 → 1 → 2 → 3 の順。フェーズ 0 が全フェーズの土台であり、ここだけは
飛ばせない。フェーズ 1 完了時点で API 経由の操作が可能になり、フェーズ 2 で
非エンジニアが触れる状態、フェーズ 3 で「張り付き不要」と配布が完成する。
