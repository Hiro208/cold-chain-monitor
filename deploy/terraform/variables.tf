variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, e.g. dev or prod."
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "kubernetes_version" {
  type    = string
  default = "1.31"
}

variable "node_instance_types" {
  description = "Graviton instance types for the EKS managed node group."
  type        = list(string)
  default     = ["m7g.large"]
}

variable "node_group_size" {
  type = object({ min = number, desired = number, max = number })
  default = {
    min     = 2
    desired = 3
    max     = 6
  }
}

variable "readings_partitions" {
  description = "Partitions of the readings topic; caps ingest parallelism."
  type        = number
  default     = 12
}

variable "partners" {
  description = "Partner name => container owners its API key may subscribe to."
  type        = map(list(string))
  default     = {}
}

variable "api_nlb_arn" {
  description = "ARN of the internal NLB the AWS Load Balancer Controller created for the coldchain-api Service (second apply)."
  type        = string
  default     = ""
}

variable "alert_email" {
  description = "Optional operator e-mail subscribed to the breach alert SNS topic."
  type        = string
  default     = ""
}
