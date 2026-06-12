output "nlb_dns_name" {
  value       = aws_lb.lineage.dns_name
  description = "Public DNS name of the lineage NLB."
}

output "lineage_url" {
  value       = local.lineage_url
  description = "Endpoint clients send OpenLineage events to (https when a domain is set, otherwise raw TCP)."
}

output "lineage_domain" {
  value       = local.enable_https ? var.domain_name : null
  description = "Custom HTTPS domain serving the lineage endpoint (null when HTTPS is disabled)."
}

output "cluster_name" {
  value       = aws_ecs_cluster.lineage.name
  description = "ECS cluster name."
}

output "service_name" {
  value       = aws_ecs_service.lineage.name
  description = "ECS service name."
}

output "task_family" {
  value       = aws_ecs_task_definition.lineage.family
  description = "ECS task definition family."
}

output "lineage_ecr_repository_url" {
  value       = data.aws_ecr_repository.lineage.repository_url
  description = "ECR repository URL for the lineage-service image."
}

output "table_ecr_repository_url" {
  value       = data.aws_ecr_repository.table.repository_url
  description = "ECR repository URL for the table-service image."
}

output "lineage_log_group" {
  value       = aws_cloudwatch_log_group.lineage.name
  description = "CloudWatch Logs group for the lineage-service container."
}

output "table_log_group" {
  value       = aws_cloudwatch_log_group.table.name
  description = "CloudWatch Logs group for the table-service container."
}
