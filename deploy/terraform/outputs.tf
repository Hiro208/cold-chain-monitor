output "cluster_name" {
  value = module.eks.cluster_name
}

output "ecr_repositories" {
  value = { for k, r in aws_ecr_repository.service : k => r.repository_url }
}

output "msk_bootstrap_brokers_tls" {
  value = aws_msk_cluster.this.bootstrap_brokers_tls
}

output "postgres_endpoint" {
  value = aws_db_instance.postgres.address
}

output "redis_endpoint" {
  value = aws_elasticache_replication_group.redis.primary_endpoint_address
}

output "workload_role_arns" {
  value = { for k, r in aws_iam_role.workload : k => r.arn }
}

output "archive_bucket" {
  value = aws_s3_bucket.archive.bucket
}

output "breach_alert_topic_arn" {
  value = aws_sns_topic.breach_alerts.arn
}

output "partner_api_url" {
  value = local.partner_api_enabled ? aws_api_gateway_stage.partner[0].invoke_url : null
}

# Partner name => API Gateway API key id. Ops records a partner_grants row
# (partner_id = this id) for every owner in var.partners.
output "partner_api_key_ids" {
  value = { for k, v in aws_api_gateway_api_key.partner : k => v.id }
}
