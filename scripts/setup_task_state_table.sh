#!/bin/bash
# Create fleet-task-state DynamoDB table

aws dynamodb create-table \
  --table-name fleet-task-state \
  --attribute-definitions AttributeName=task_id,AttributeType=S \
  --key-schema AttributeName=task_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-northeast-1

echo "✓ fleet-task-state table created"
