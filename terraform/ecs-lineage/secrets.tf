# ----- Secrets (sensitive values injected at container start) ----------------

# Fallback UC service token used by the table-service for credential vending
# when no per-user JWT is forwarded.
resource "aws_secretsmanager_secret" "unity_catalog_token" {
  count                   = var.unity_catalog_token == "" ? 0 : 1
  name                    = "${var.name_prefix}/unity-catalog-token"
  recovery_window_in_days = 0
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "unity_catalog_token" {
  count         = var.unity_catalog_token == "" ? 0 : 1
  secret_id     = aws_secretsmanager_secret.unity_catalog_token[0].id
  secret_string = var.unity_catalog_token
}

# Expected token for static auth mode on the lineage service.
resource "aws_secretsmanager_secret" "lineage_auth_token" {
  count                   = var.lineage_auth_token == "" ? 0 : 1
  name                    = "${var.name_prefix}/lineage-auth-token"
  recovery_window_in_days = 0
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "lineage_auth_token" {
  count         = var.lineage_auth_token == "" ? 0 : 1
  secret_id     = aws_secretsmanager_secret.lineage_auth_token[0].id
  secret_string = var.lineage_auth_token
}

locals {
  secret_arns = concat(
    var.unity_catalog_token == "" ? [] : [aws_secretsmanager_secret.unity_catalog_token[0].arn],
    var.lineage_auth_token == "" ? [] : [aws_secretsmanager_secret.lineage_auth_token[0].arn],
  )
}
