output "region" {
  value = var.region
}

output "sqs_name" {
  value = aws_sqs_queue.fleet_queue.name
}

output "state_table" {
  value = aws_dynamodb_table.state.name
}

output "budget_table" {
  value = aws_dynamodb_table.budget.name
}

output "budget_guard_table" {
  value = aws_dynamodb_table.budget_guard.name
}

output "index_table" {
  value = aws_dynamodb_table.index.name
}

output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}
