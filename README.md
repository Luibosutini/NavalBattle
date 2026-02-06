# Naval Battle - AI Fleet Management System

AWS-based AI code generation system using naval battle metaphor.

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
- `FleetNodePolicy.json` - IAM policy for fleet-node user
- `requirements.txt` - Python dependencies

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
  --policy-document file://FleetNodePolicy.json
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
export BEDROCK_SONNET_MODEL_ID=anthropic.claude-3-5-sonnet-20240620-v1:0
export BEDROCK_OPUS_MODEL_ID=anthropic.claude-3-opus-20240229-v1:0
export BEDROCK_MICRO_MODEL_ID=amazon.nova-micro-v1:0
export BEDROCK_LITE_MODEL_ID=amazon.nova-lite-v1:0
export BEDROCK_MISTRAL_MODEL_ID=mistral.mistral-7b-instruct-v0:2
```

**PowerShell (Windows):**
```powershell
# Use start_fleet.ps1 script (edit placeholders first)
.\start_fleet.ps1
```

4. **Run:**
```bash
# Linux/macOS
python fleet_node.py

# Windows (PowerShell)
.\start_fleet.ps1
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

## Budget Management

- **Fuel**: $30/month (tracks all operations)
- **Ammo**:
  - BB Main Gun: 4/month
  - CA Salvo: 40/month
  - CVB Air Wing: 20/month

## License

MIT
