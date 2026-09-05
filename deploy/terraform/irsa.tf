# Least-privilege IAM roles assumed by the Kubernetes service accounts in
# deploy/k8s/serviceaccounts.yaml.

data "aws_iam_policy_document" "irsa_trust" {
  for_each = toset(["api", "ingest", "archiver"])

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:coldchain:coldchain-${each.key}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "workload" {
  for_each           = data.aws_iam_policy_document.irsa_trust
  name               = "coldchain-${each.key}"
  assume_role_policy = each.value.json
}
