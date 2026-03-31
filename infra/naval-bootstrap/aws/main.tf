terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  sqs_name              = "${var.name_prefix}-queue"
  state_table_name      = "${var.name_prefix}-state"
  budget_table_name     = "${var.name_prefix}-budget"
  budget_guard_name     = "${var.name_prefix}-budget-guard"
  index_table_name      = "${var.name_prefix}-index"
  artifact_bucket_name  = "${var.name_prefix}-artifacts"
}

resource "aws_sqs_queue" "fleet_queue" {
  name = local.sqs_name
}

resource "aws_dynamodb_table" "state" {
  name         = local.state_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "mission_id"
  range_key    = "task_id"

  attribute {
    name = "mission_id"
    type = "S"
  }

  attribute {
    name = "task_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "budget" {
  name         = local.budget_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "mission_id"

  attribute {
    name = "mission_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "budget_guard" {
  name         = local.budget_guard_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "mission_id"

  attribute {
    name = "mission_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "index" {
  name         = local.index_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "mission_id"
  range_key    = "task_id"

  attribute {
    name = "mission_id"
    type = "S"
  }

  attribute {
    name = "task_id"
    type = "S"
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = local.artifact_bucket_name
  force_destroy = var.force_destroy
}
