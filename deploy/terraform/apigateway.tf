# Partner-facing API: /v1/webhooks behind API Gateway with API keys and a
# usage plan (throttling + quota per partner). Requests reach the EKS API
# pods through a VPC link to the internal NLB. The integration overwrites
# X-Partner-Id with the caller's API key id, which the service uses to scope
# subscriptions — clients cannot spoof it.
#
# The NLB is created by the AWS Load Balancer Controller from the
# coldchain-api Service, so this file is applied once var.api_nlb_arn is set.

locals {
  partner_api_enabled = var.api_nlb_arn != ""
}

data "aws_lb" "api" {
  count = local.partner_api_enabled ? 1 : 0
  arn   = var.api_nlb_arn
}

resource "aws_api_gateway_vpc_link" "api" {
  count       = local.partner_api_enabled ? 1 : 0
  name        = "${local.name}-api"
  target_arns = [var.api_nlb_arn]
}

resource "aws_api_gateway_rest_api" "partner" {
  count = local.partner_api_enabled ? 1 : 0
  name  = "${local.name}-partner"
  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "v1" {
  count       = local.partner_api_enabled ? 1 : 0
  rest_api_id = aws_api_gateway_rest_api.partner[0].id
  parent_id   = aws_api_gateway_rest_api.partner[0].root_resource_id
  path_part   = "v1"
}

resource "aws_api_gateway_resource" "webhooks" {
  count       = local.partner_api_enabled ? 1 : 0
  rest_api_id = aws_api_gateway_rest_api.partner[0].id
  parent_id   = aws_api_gateway_resource.v1[0].id
  path_part   = "webhooks"
}

resource "aws_api_gateway_resource" "webhook" {
  count       = local.partner_api_enabled ? 1 : 0
  rest_api_id = aws_api_gateway_rest_api.partner[0].id
  parent_id   = aws_api_gateway_resource.webhooks[0].id
  path_part   = "{id}"
}

locals {
  # method key => resource, HTTP method and backend path
  partner_routes = local.partner_api_enabled ? {
    list   = { resource = aws_api_gateway_resource.webhooks[0].id, method = "GET", path = "/v1/webhooks", has_id = false }
    create = { resource = aws_api_gateway_resource.webhooks[0].id, method = "POST", path = "/v1/webhooks", has_id = false }
    delete = { resource = aws_api_gateway_resource.webhook[0].id, method = "DELETE", path = "/v1/webhooks/{id}", has_id = true }
  } : {}
}

resource "aws_api_gateway_method" "partner" {
  for_each         = local.partner_routes
  rest_api_id      = aws_api_gateway_rest_api.partner[0].id
  resource_id      = each.value.resource
  http_method      = each.value.method
  authorization    = "NONE"
  api_key_required = true

  request_parameters = each.value.has_id ? { "method.request.path.id" = true } : {}
}

resource "aws_api_gateway_integration" "partner" {
  for_each                = local.partner_routes
  rest_api_id             = aws_api_gateway_rest_api.partner[0].id
  resource_id             = each.value.resource
  http_method             = aws_api_gateway_method.partner[each.key].http_method
  type                    = "HTTP_PROXY"
  integration_http_method = each.value.method
  connection_type         = "VPC_LINK"
  connection_id           = aws_api_gateway_vpc_link.api[0].id
  uri                     = "http://${data.aws_lb.api[0].dns_name}${each.value.path}"
  timeout_milliseconds    = 10000

  request_parameters = merge(
    { "integration.request.header.X-Partner-Id" = "context.identity.apiKeyId" },
    each.value.has_id ? { "integration.request.path.id" = "method.request.path.id" } : {},
  )
}

resource "aws_api_gateway_deployment" "partner" {
  count       = local.partner_api_enabled ? 1 : 0
  rest_api_id = aws_api_gateway_rest_api.partner[0].id

  triggers = {
    redeploy = sha1(jsonencode([aws_api_gateway_method.partner, aws_api_gateway_integration.partner]))
  }
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "partner" {
  count         = local.partner_api_enabled ? 1 : 0
  rest_api_id   = aws_api_gateway_rest_api.partner[0].id
  deployment_id = aws_api_gateway_deployment.partner[0].id
  stage_name    = "v1"
}

resource "aws_api_gateway_usage_plan" "partner" {
  count = local.partner_api_enabled ? 1 : 0
  name  = "${local.name}-partner-standard"

  api_stages {
    api_id = aws_api_gateway_rest_api.partner[0].id
    stage  = aws_api_gateway_stage.partner[0].stage_name
  }

  throttle_settings {
    rate_limit  = 10
    burst_limit = 20
  }

  quota_settings {
    limit  = 10000
    period = "DAY"
  }
}

resource "aws_api_gateway_api_key" "partner" {
  for_each = local.partner_api_enabled ? var.partners : {}
  name     = "${local.name}-${each.key}"
}

resource "aws_api_gateway_usage_plan_key" "partner" {
  for_each      = aws_api_gateway_api_key.partner
  key_id        = each.value.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.partner[0].id
}
