# Naval Battle - AI Fleet Management System

AWS-based AI code generation system using naval battle metaphor.

Japanese version: `README.ja.md`

## Architecture

**艦隊ドクトリン（Fleet Doctrine）:**
- **CVL** (Light Carrier): Recon/Compression - Nova Micro
- **DD** (Destroyer): Implementation - Mistral/Sonnet
- **CL** (Light Cruiser): Testing - Nova Lite
- **CVB** (Armored Carrier): Verification Campaign - Micro/Lite Air Wing
- **CA** (Heavy Cruiser): Fleet Commander - Sonnet 4.5
- **BB** (Battleship): Decisive Strike - Opus 4.5

## AWS Services

- **SQS**: Mission queue
- **DynamoDB**: State management & budget tracking
- **S3**: Artifact storage (tickets, results)
- **Bedrock**: AI models (Claude Sonnet/Opus, Nova Micro/Lite)

## Files

### Core
- `fleet_node.py` - Main worker node
- `enqueue.py` - Mission enqueuer
- `dispatch_capital.py` - Capital ship dispatcher
- `test_report.py` - Test results reporter

### Configuration
- `policies/FleetNodePolicy.json` - IAM policy for fleet-node user
- `requirements.txt` - Python dependencies

### Supporting
- `docs/` - Architecture, operations, and troubleshooting docs
- `docs/notes/` - Bedrock and mission notes
- `scripts/` - PowerShell/Bash helper scripts
- `samples/` - Sample tickets and misc files
- `runtime/logs/` - Runtime log output

## Setup

**IMPORTANT: Replace placeholders before use:**
- `YOUR_ACCOUNT_ID` → Your AWS Account ID
- `YOUR_BUCKET_NAME` → Your S3 bucket name (e.g., `fleet-tokyo-artifacts-xxxxx`)
- `your/repo.git` → Your GitHub repository URL

1. **AWS Resources:**
```bash
# Create SQS queue
aws sqs create-queue --queue-name fleet-missions

# Create DynamoDB tables
aws dynamodb create-table --table-name fleet-mission-state \
  --attribute-definitions AttributeName=mission_id,AttributeType=S \
  --key-schema AttributeName=mission_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb create-table --table-name fleet-budget \
  --attribute-definitions AttributeName=month,AttributeType=S \
  --key-schema AttributeName=month,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Create S3 bucket (replace XXXXX with random string)
aws s3 mb s3://fleet-tokyo-artifacts-XXXXX
```

2. **IAM User:**
```bash
aws iam create-user --user-name fleet-node
aws iam put-user-policy --user-name fleet-node \
  --policy-name FleetNodePolicy \
  --policy-document file://policies/FleetNodePolicy.json
```

3. **Environment Variables:**
```bash
export FLEET_REGION=ap-northeast-1
export FLEET_SQS_NAME=fleet-missions
export FLEET_STATE_TABLE=fleet-mission-state
export FLEET_BUDGET_TABLE=fleet-budget
export FLEET_S3_BUCKET=fleet-tokyo-artifacts-XXXXX  # Replace XXXXX
export REPO_URL=https://github.com/your/repo.git    # Replace with your repo

# Bedrock Model IDs (verified for ap-northeast-1)
export BEDROCK_SONNET_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/jp.anthropic.claude-sonnet-4-5-20250929-v1:0
export BEDROCK_OPUS_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/global.anthropic.claude-opus-4-6-v1
export BEDROCK_MICRO_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/apac.amazon.nova-micro-v1:0
export BEDROCK_LITE_MODEL_ID=arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/apac.amazon.nova-lite-v1:0
export BEDROCK_MISTRAL_MODEL_ID=mistral.mistral-7b-instruct-v0:2
```

**PowerShell (Windows):**
```powershell
# Use start_fleet.ps1 script (edit placeholders first)
.\scripts\start_fleet.ps1
```

4. **Run:**
```bash
# Linux/macOS
python fleet_node.py

# Windows (PowerShell)
.\scripts\start_fleet.ps1
```

## Bedrock Model Access

Enable model access in AWS Console:
1. Go to AWS Bedrock → Model access
2. Enable the following models:
   - Claude 3.5 Sonnet v1
   - Claude 3 Opus
   - Amazon Nova Micro
   - Amazon Nova Lite
   - Mistral 7B Instruct (optional, for DD)

## Usage

```bash
# Enqueue a mission
python enqueue.py --task T-001 --ship CVL-01 --ticket "Implement feature X"

# Check results
python test_report.py
```

## Local Workspace & Logs

- `config.yaml` defines ship mental models and local workspace layout.
- `navalctl` provides local workspace creation and log search.

```bash
python navalctl.py init
python navalctl.py mkws --mission-id M-XXX --task-id T-XXX --ship-class CVL
python navalctl.py search --query "NEED_INPUT"
```

## Budget Management

- **Fuel**: $30/month (tracks all operations)
- **Ammo**:
  - BB Main Gun: 4/month
  - CA Salvo: 40/month
  - CVB Air Wing: 20/month

## License

MIT
