data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ----- Networking: default VPC + its subnets unless overridden ---------------
data "aws_vpc" "default" {
  count   = var.vpc_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [local.vpc_id]
  }
}

# Resolve the chosen VPC (for its CIDR — used by the task SG to admit NLB traffic).
data "aws_vpc" "selected" {
  id = local.vpc_id
}

# ECR repositories are created by the push scripts; Terraform only references them.
data "aws_ecr_repository" "lineage" {
  name = var.lineage_ecr_repo_name
}

data "aws_ecr_repository" "table" {
  name = var.table_ecr_repo_name
}

locals {
  vpc_id     = var.vpc_id != "" ? var.vpc_id : data.aws_vpc.default[0].id
  subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : tolist(data.aws_subnets.default[0].ids)
  vpc_cidr   = data.aws_vpc.selected.cidr_block

  lineage_image_uri = "${data.aws_ecr_repository.lineage.repository_url}:${var.lineage_image_tag}"
  table_image_uri   = "${data.aws_ecr_repository.table.repository_url}:${var.table_image_tag}"

  enable_https = var.domain_name != "" && var.hosted_zone_id != ""

  # Public lineage endpoint, surfaced as an output. ConnectRPC clients use
  # https when a domain is configured, otherwise raw TCP on the NLB DNS name.
  lineage_url = local.enable_https ? "https://${var.domain_name}" : "tcp://${aws_lb.lineage.dns_name}:${var.lineage_port}"
}

# ----- Logs ------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "lineage" {
  name              = "/ecs/${var.name_prefix}/lineage-service"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "table" {
  name              = "/ecs/${var.name_prefix}/table-service"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}
