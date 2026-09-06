# Runtime configuration for the EKS workloads, synced into the
# coldchain-secrets Kubernetes Secret by External Secrets Operator.
# DATABASE_URL is composed by ESO from the RDS-managed master secret.
resource "aws_secretsmanager_secret" "runtime" {
  name = "coldchain/${var.environment}/runtime"
}

# AES-256 key that encrypts partner webhook signing secrets at rest.
resource "random_bytes" "webhook_key" {
  length = 32
}

resource "aws_secretsmanager_secret_version" "runtime" {
  secret_id = aws_secretsmanager_secret.runtime.id
  secret_string = jsonencode({
    KAFKA_BROKERS      = aws_msk_cluster.this.bootstrap_brokers_tls
    KAFKA_TLS          = "true"
    REDIS_ADDR         = "${aws_elasticache_replication_group.redis.primary_endpoint_address}:6379"
    WEBHOOK_SECRET_KEY = random_bytes.webhook_key.base64
    ARCHIVE_BUCKET     = aws_s3_bucket.archive.bucket
  })
}
