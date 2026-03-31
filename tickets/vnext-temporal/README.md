# NavalBattle vNext Temporal Backlog

このディレクトリは、`docs/VNEXT_TEMPORAL_SPEC.ja.md` を実装するためのチケット一式です。

## 進め方
- `P0` を優先し、Walking Skeletonを最短で通す。
- 各チケットは `受入条件` を満たした時点で完了とする。
- 依存関係があるチケットは、先行チケット完了後に着手する。

## チケット一覧
| ID | 優先度 | タイトル | 依存 | 状態 |
|---|---|---|---|---|
| [T-VNEXT-000](./T-VNEXT-000.md) | P0 | Epic: Temporal Edition 実装 | - | DONE |
| [T-VNEXT-001](./T-VNEXT-001.md) | P0 | CLI基盤（Typer）とコマンド骨格 | 000 | DONE |
| [T-VNEXT-002](./T-VNEXT-002.md) | P0 | `naval doctor` 実装 | 001 | DONE |
| [T-VNEXT-003](./T-VNEXT-003.md) | P0 | Runtime抽象化（AWS/Temporal差し替え） | 001 | DONE |
| [T-VNEXT-004](./T-VNEXT-004.md) | P0 | Temporal Worker/Workflow雛形（Hello Mission） | 003 | DONE |
| [T-VNEXT-005](./T-VNEXT-005.md) | P0 | MissionWorkflow状態モデルとHITL Signal | 004 | DONE |
| [T-VNEXT-006](./T-VNEXT-006.md) | P0 | 必須Activity群（Event/Comms/Artifact/Index/Budget） | 005 | DONE |
| [T-VNEXT-007](./T-VNEXT-007.md) | P0 | Bedrock Activity + Retry + コスト計測 | 006 | DONE |
| [T-VNEXT-008](./T-VNEXT-008.md) | P0 | Artifacts契約（Pydantic）導入 | 006 | DONE |
| [T-VNEXT-009](./T-VNEXT-009.md) | P0 | `naval enqueue/status` 実装 | 005, 008 | DONE |
| [T-VNEXT-010](./T-VNEXT-010.md) | P0 | `naval approve/input/tail/pull` 実装 | 009 | DONE |
| [T-VNEXT-011](./T-VNEXT-011.md) | P1 | `naval watch`（TUI）実装 | 010 | DONE |
| [T-VNEXT-012](./T-VNEXT-012.md) | P1 | `context` 共有プロトコル実装 | 006 | DONE |
| [T-VNEXT-013](./T-VNEXT-013.md) | P1 | Budget Guard強化（Interceptor + Circuit Breaker） | 007 | DONE |
| [T-VNEXT-014](./T-VNEXT-014.md) | P1 | `naval up` Bootstrap（local/aws） | 002 | DONE |
| [T-VNEXT-015](./T-VNEXT-015.md) | P0 | E2E受入試験とドキュメント更新 | 010 | DONE |

## Walking Skeleton（最小垂直統合）
1. `T-VNEXT-001`
2. `T-VNEXT-002`
3. `T-VNEXT-003`
4. `T-VNEXT-004`
5. `T-VNEXT-009`
6. `T-VNEXT-010`
