# ----- Task security group ---------------------------------------------------
# The NLB has no security group of its own, so the lineage port is admitted
# from the VPC CIDR (the NLB forwards from within the VPC). The two containers
# share the task's loopback, so no intra-task rule is needed; the self rule is
# kept for parity / future multi-task topologies.
resource "aws_security_group" "task" {
  name        = "${var.name_prefix}-task"
  description = "open-lineage Fargate task: NLB ingress to the lineage port."
  vpc_id      = local.vpc_id
  tags        = var.tags

  ingress {
    description = "All traffic between tasks in this SG"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "Lineage ConnectRPC from the NLB (VPC CIDR; NLB has no SG)"
    from_port   = var.lineage_port
    to_port     = var.lineage_port
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
  }

  egress {
    description = "Outbound to ECR, S3, STS, Unity Catalog, logs, and the open web"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
