resource "aws_ecs_cluster" "lineage" {
  name = "${var.name_prefix}-cluster"
  tags = var.tags

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

locals {
  lineage_env = [
    { name = "PORT", value = tostring(var.lineage_port) },
    { name = "TABLE_SERVICE_URL", value = "http://localhost:${var.table_port}" },
    { name = "LINEAGE_AUTH_MODE", value = var.lineage_auth_mode },
    { name = "UNITY_CATALOG_URL", value = var.unity_catalog_url },
  ]

  lineage_secrets = var.lineage_auth_token == "" ? [] : [
    { name = "LINEAGE_AUTH_TOKEN", valueFrom = aws_secretsmanager_secret.lineage_auth_token[0].arn },
  ]

  table_env = [
    { name = "TABLE_SERVICE_PORT", value = tostring(var.table_port) },
    { name = "DELTA_STORAGE", value = var.delta_storage },
    { name = "DELTA_TABLE_PATH", value = var.delta_table_path },
    { name = "DELTA_PARTITION_COLS", value = var.delta_partition_cols },
    { name = "UNITY_CATALOG_URL", value = var.unity_catalog_url },
    { name = "UC_CATALOG", value = var.uc_catalog },
    { name = "UC_SCHEMA", value = var.uc_schema },
    { name = "UC_TABLE", value = var.uc_table },
    { name = "AWS_REGION", value = var.aws_region },
    { name = "RUST_LOG", value = var.rust_log },
    { name = "SERVICE_VERSION", value = var.table_image_tag },
  ]

  table_secrets = var.unity_catalog_token == "" ? [] : [
    { name = "UNITY_CATALOG_TOKEN", valueFrom = aws_secretsmanager_secret.unity_catalog_token[0].arn },
  ]
}

resource "aws_ecs_task_definition" "lineage" {
  family                   = var.name_prefix
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.task_cpu)
  memory                   = tostring(var.task_memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn
  tags                     = var.tags

  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name        = "table-service"
      image       = local.table_image_uri
      essential   = true
      environment = local.table_env
      secrets     = local.table_secrets
      portMappings = [
        { containerPort = var.table_port, protocol = "tcp" },
      ]
      healthCheck = {
        command     = ["CMD-SHELL", "curl -sf http://localhost:${var.table_port}/health || exit 1"]
        interval    = 10
        timeout     = 5
        retries     = 3
        startPeriod = 15
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.table.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    },
    {
      name        = "lineage-service"
      image       = local.lineage_image_uri
      essential   = true
      environment = local.lineage_env
      secrets     = local.lineage_secrets
      portMappings = [
        { containerPort = var.lineage_port, protocol = "tcp" },
      ]
      dependsOn = [
        { containerName = "table-service", condition = "HEALTHY" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.lineage.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    },
  ])
}

resource "aws_ecs_service" "lineage" {
  name                   = var.name_prefix
  cluster                = aws_ecs_cluster.lineage.id
  task_definition        = aws_ecs_task_definition.lineage.arn
  desired_count          = var.desired_count
  launch_type            = "FARGATE"
  enable_execute_command = true
  tags                   = var.tags

  network_configuration {
    subnets          = local.subnet_ids
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.lineage.arn
    container_name   = "lineage-service"
    container_port   = var.lineage_port
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  depends_on = [
    aws_lb_listener.lineage,
  ]
}
