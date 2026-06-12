# ----- IAM: task execution role (pull image, write logs, read secrets) -------
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name_prefix}-exec"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Allow the execution role to read the container secrets at task start.
data "aws_iam_policy_document" "execution_secrets" {
  count = length(local.secret_arns) == 0 ? 0 : 1
  statement {
    sid       = "ReadContainerSecrets"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = local.secret_arns
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  count  = length(local.secret_arns) == 0 ? 0 : 1
  name   = "read-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets[0].json
}

# ----- IAM: task role (ECS Exec + optional direct S3 access) -----------------
resource "aws_iam_role" "task" {
  name               = "${var.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "task_exec" {
  statement {
    sid    = "SSMExec"
    effect = "Allow"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "task_exec" {
  name   = "ecs-exec"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_exec.json
}

# Optional: read/write the Delta bucket directly when DELTA_STORAGE=s3. The
# Unity Catalog write path vends its own scoped temporary credentials and does
# not rely on this policy.
data "aws_iam_policy_document" "task_s3" {
  count = var.delta_bucket == "" ? 0 : 1

  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.delta_bucket}"]
  }

  statement {
    sid       = "ObjectRW"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:aws:s3:::${var.delta_bucket}/*"]
  }
}

resource "aws_iam_role_policy" "task_s3" {
  count  = var.delta_bucket == "" ? 0 : 1
  name   = "delta-s3"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_s3[0].json
}
