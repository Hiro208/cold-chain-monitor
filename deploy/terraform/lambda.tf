# Breach alert routing: MSK (reefer.breaches.v1) -> Lambda -> SNS + partner
# webhooks. Build the artifact with `make lambda` before applying.

resource "aws_sns_topic" "breach_alerts" {
  name              = "${local.name}-breach-alerts"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "ops_email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.breach_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Batches that still fail after the retry budget land here for inspection
# and manual redrive instead of blocking the partition forever.
resource "aws_sqs_queue" "breach_alert_dlq" {
  name                      = "${local.name}-breach-alert-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_security_group" "lambda" {
  name_prefix = "${local.name}-lambda-"
  vpc_id      = module.vpc.vpc_id

  egress {
    description = "MSK, RDS Proxy, AWS APIs and partner webhooks via NAT"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "lambda_to_postgres" {
  type                     = "ingress"
  security_group_id        = aws_security_group.postgres.id
  source_security_group_id = aws_security_group.lambda.id
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  description              = "Alert Lambda via RDS Proxy"
}

resource "aws_security_group_rule" "lambda_to_msk" {
  type                     = "ingress"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = aws_security_group.lambda.id
  from_port                = 9094
  to_port                  = 9094
  protocol                 = "tcp"
  description              = "Alert Lambda event source mapping"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "breach_alert" {
  name               = "${local.name}-breach-alert"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# ENI management in the VPC + reading from MSK.
resource "aws_iam_role_policy_attachment" "breach_alert_msk" {
  role       = aws_iam_role.breach_alert.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaMSKExecutionRole"
}

resource "aws_iam_role_policy" "breach_alert" {
  role = aws_iam_role.breach_alert.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sns:Publish"], Resource = [aws_sns_topic.breach_alerts.arn] },
      { Effect = "Allow", Action = ["sqs:SendMessage"], Resource = [aws_sqs_queue.breach_alert_dlq.arn] },
      { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [aws_secretsmanager_secret.runtime.arn] },
    ]
  })
}

resource "aws_cloudwatch_log_group" "breach_alert" {
  name              = "/aws/lambda/${local.name}-breach-alert"
  retention_in_days = 30
}

resource "aws_lambda_function" "breach_alert" {
  function_name = "${local.name}-breach-alert"
  role          = aws_iam_role.breach_alert.arn
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  handler       = "bootstrap"
  filename      = "${path.module}/../../lambda/breachalert/breachalert.zip"

  source_code_hash = filebase64sha256("${path.module}/../../lambda/breachalert/breachalert.zip")
  memory_size      = 256
  timeout          = 60

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      ALERT_TOPIC_ARN = aws_sns_topic.breach_alerts.arn
      # Password is injected at deploy time from the RDS-managed secret;
      # the endpoint is the proxy, not the instance.
      DATABASE_URL       = "postgres://coldchain@${aws_db_proxy.postgres.endpoint}:5432/coldchain?sslmode=require"
      WEBHOOK_SECRET_KEY = random_bytes.webhook_key.base64
      LOG_FORMAT         = "json"
    }
  }

  depends_on = [aws_cloudwatch_log_group.breach_alert]
}

resource "aws_lambda_event_source_mapping" "breaches" {
  function_name     = aws_lambda_function.breach_alert.arn
  event_source_arn  = aws_msk_cluster.this.arn
  topics            = ["reefer.breaches.v1"]
  starting_position = "LATEST"

  batch_size                         = 50
  maximum_batching_window_in_seconds = 1 # keeps end-to-end alerting fast

  amazon_managed_kafka_event_source_config {
    consumer_group_id = "reefer-breach-alert"
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.breach_alert_dlq.arn
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "breach_alert_errors" {
  alarm_name          = "${local.name}-breach-alert-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.breach_alert.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.breach_alerts.arn]
}
