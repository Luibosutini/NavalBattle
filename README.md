# Naval Battle - AI Fleet Management System

AWS-based AI code generation system using a naval battle metaphor.
Japanese docs: `README.ja.md`

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy and fill in the variables:

```bash
export FLEET_REGION=ap-northeast-1
export FLEET_SQS_NAME=fleet-missions
export FLEET_STATE_TABLE=fleet-mission-state
export FLEET_BUDGET_TABLE=fleet-budget
export FLEET_TASK_STATE_TABLE=fleet-task-state
export FLEET_S3_BUCKET=fleet-tokyo-artifacts-XXXXX
export REPO_URL=https://github.com/your/repo.git

# Bedrock model IDs (ap-northeast-1)
export BEDROCK_SONNET_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/jp.anthropic.claude-sonnet-4-5-20250929-v1:0
export BEDROCK_OPUS_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/global.anthropic.claude-opus-4-6-v1
export BEDROCK_MICRO_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/apac.amazon.nova-micro-v1:0
export BEDROCK_LITE_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/apac.amazon.nova-lite-v1:0
```

Or use the bootstrap command to generate a `.env` file:

```bash
python -m naval up --profile aws --dry-run
```

### 3. Check the environment

```bash
python -m naval doctor
```

### 4. Start the worker

```bash
# Linux/macOS
python fleet_node.py

# Windows (PowerShell)
.\scripts\start_fleet.ps1
```

---

## CLI Reference (`python -m naval`)

All operations go through the unified `naval` CLI.

```
python -m naval [--runtime aws|temporal] <command> [options]
```

| Command | Description |
|---------|-------------|
| `doctor` | Check AWS connectivity and environment variables |
| `up` | Bootstrap local or AWS environment |
| `enqueue` | Submit a new mission |
| `pending` | **Interactive HITL** — list and respond to NEED_INPUT / NEED_APPROVAL missions |
| `approve` | Approve a specific mission by ID |
| `input` | Send a text response to a NEED_INPUT mission |
| `status` | Show mission status |
| `watch` | Real-time dashboard (rich TUI) |
| `tail` | Show recent logs |
| `pull` | Download mission artifacts from S3 |
| `ca` | Send a CA directive to an in-progress task |
| `ws` | Local workspace management |

### Enqueue a mission

```bash
python -m naval enqueue --ticket "Implement feature X" --repo-url https://github.com/org/repo.git
# With formation
python -m naval enqueue --ticket tickets/T-0001.md --doctrine standard_full --watch
```

### Handle pending missions (interactive)

```bash
python -m naval pending
```

Scans for NEED_INPUT / NEED_APPROVAL missions, displays a numbered list, and guides you through approval or multi-line input.

### Approve / send input directly

```bash
python -m naval approve --mission MS-20260401-123456 --yes
python -m naval approve --mission MS-20260401-123456 --no --note "needs rework"
python -m naval input  --mission MS-20260401-123456 "All tests pass now."
```

### Watch dashboard

```bash
python -m naval watch                  # refresh every 2 s
python -m naval watch --interval 5 --filter NEED_APPROVAL
```

Left pane: mission list sorted by urgency (NEED_* highlighted in red).
Right pane: recent comms for the selected mission.

### Pull artifacts

```bash
python -m naval pull --mission MS-20260401-123456
python -m naval pull --mission MS-20260401-123456 --out ./results/
```

### CA directive

```bash
# Interactive
python -m naval ca T-0001

# One-liner
python -m naval ca T-0001 --directive "Execute: DD_SONNET, DD_MISTRAL" --repo-url https://github.com/org/repo.git

# Save only, skip auto-advance
python -m naval ca T-0001 --directive "Skip: BB" --no-advance
```

### Local workspace (`ws`)

```bash
python -m naval ws init
python -m naval ws mkws --mission-id M-XXX --task-id T-XXX --ship-class CVL
python -m naval ws mental --ship-class CA
python -m naval ws search --query "NEED_INPUT"
python -m naval ws tail --limit 30
python -m naval ws info
```

---

## Temporal Runtime (vNext)

Use `--runtime temporal` for the Temporal-based backend.

```bash
# Bootstrap local Temporal
python -m naval up --profile local --apply

# Start worker
python naval_worker.py

# Enqueue with HITL gate
python -m naval --runtime temporal enqueue --ticket "Implement X" --need-approval --watch

# Approve via signal
python -m naval --runtime temporal approve --mission <mission_id> --yes

# Real-time TUI watch
python -m naval --runtime temporal watch --interval 2
```

---

## Architecture

**Fleet Doctrine:**

| Class | Role | Model |
|-------|------|-------|
| CVL (Light Carrier) | Recon / Compression | Nova Micro |
| DD (Destroyer) | Implementation | Mistral / Sonnet |
| CL (Light Cruiser) | Testing | Nova Lite |
| CVB (Armored Carrier) | Verification | Micro / Lite Air Wing |
| CA (Heavy Cruiser) | Fleet Commander | Sonnet 4.5 |
| BB (Battleship) | Decisive Strike | Opus 4.5 |

**AWS Services:**

| Service | Purpose |
|---------|---------|
| SQS | Mission queue |
| DynamoDB | State management (`fleet-mission-state`, `fleet-task-state`, `fleet-budget`) |
| S3 | Artifact storage (`missions/`, `tickets/`, `results/`) |
| Bedrock | AI model invocation |

---

## AWS Setup

```bash
# SQS
aws sqs create-queue --queue-name fleet-missions

# DynamoDB
aws dynamodb create-table --table-name fleet-mission-state \
  --attribute-definitions AttributeName=mission_id,AttributeType=S \
  --key-schema AttributeName=mission_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb create-table --table-name fleet-task-state \
  --attribute-definitions AttributeName=task_id,AttributeType=S \
  --key-schema AttributeName=task_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb create-table --table-name fleet-budget \
  --attribute-definitions AttributeName=month,AttributeType=S \
  --key-schema AttributeName=month,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# S3
aws s3 mb s3://fleet-tokyo-artifacts-XXXXX

# IAM
aws iam create-user --user-name fleet-node
aws iam put-user-policy --user-name fleet-node \
  --policy-name FleetNodePolicy \
  --policy-document file://policies/FleetNodePolicy.json
```

---

## Budget Management

| Resource | Limit |
|----------|-------|
| Fuel | $30 / month |
| BB Main Gun | 4 / month |
| CA Salvo | 40 / month |
| CVB Air Wing | 20 / month |

---

## Docs

| File | Contents |
|------|----------|
| `docs/ARCHITECTURE.md` | System design and data schemas |
| `docs/OPERATIONS.md` | Day-to-day operations guide |
| `docs/CLI_ARGUMENTS.ja.md` | Full CLI argument reference (Japanese) |
| `docs/TROUBLESHOOTING.md` | Common issues and fixes |

---

## License

MIT
