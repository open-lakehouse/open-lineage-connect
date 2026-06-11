variable "aws_region" {
  type        = string
  description = "AWS region to deploy into."
}

variable "name_prefix" {
  type        = string
  default     = "open-lineage"
  description = "Prefix applied to all resource names."
}

# ----- Images ----------------------------------------------------------------
variable "lineage_ecr_repo_name" {
  type        = string
  default     = "lineage-service"
  description = "ECR repository holding the Go lineage-service image (created by the push script)."
}

variable "table_ecr_repo_name" {
  type        = string
  default     = "table-service"
  description = "ECR repository holding the Rust table-service image (created by the push script)."
}

variable "lineage_image_tag" {
  type        = string
  default     = "v0.1.0"
  description = "Image tag for the lineage-service container (independent of the table-service tag)."
}

variable "table_image_tag" {
  type        = string
  default     = "v0.1.0"
  description = "Image tag for the table-service container (independent of the lineage-service tag)."
}

# ----- Networking ------------------------------------------------------------
variable "vpc_id" {
  type        = string
  default     = ""
  description = "VPC to deploy into. Empty uses the account default VPC."
}

variable "subnet_ids" {
  type        = list(string)
  default     = []
  description = "Subnets for the task + NLB. Empty uses all subnets in the VPC."
}

# ----- Task sizing -----------------------------------------------------------
variable "task_cpu" {
  type        = number
  default     = 1024
  description = "Fargate task CPU units (shared by both containers)."
}

variable "task_memory" {
  type        = number
  default     = 2048
  description = "Fargate task memory (MiB, shared by both containers)."
}

variable "desired_count" {
  type        = number
  default     = 1
  description = "Number of ECS tasks to run."
}

variable "lineage_port" {
  type        = number
  default     = 8090
  description = "Port the Go lineage ConnectRPC service listens on (fronted by the NLB)."
}

variable "table_port" {
  type        = number
  default     = 8091
  description = "Port the Rust table-service sidecar listens on (task-internal only)."
}

# ----- Storage / Delta -------------------------------------------------------
variable "delta_storage" {
  type        = string
  default     = "unity"
  description = "table-service DELTA_STORAGE backend: local | s3 | unity."
}

variable "delta_bucket" {
  type        = string
  default     = ""
  description = "S3 bucket for Delta data when delta_storage=s3 (also grants the task role R/W on it)."
}

variable "delta_table_path" {
  type        = string
  default     = ""
  description = "DELTA_TABLE_PATH for delta_storage=s3 (e.g. s3://bucket/lineage-events). Ignored for unity."
}

variable "delta_partition_cols" {
  type        = string
  default     = "event_kind"
  description = "Comma-separated Delta partition columns."
}

# ----- Unity Catalog ---------------------------------------------------------
variable "unity_catalog_url" {
  type        = string
  default     = "https://uc.openlakehousedemos.dev"
  description = "Unity Catalog base URL used for JWT validation (lineage) and credential vending (table-service)."
}

variable "unity_catalog_token" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Fallback UC service bearer token for vending when no per-user JWT is forwarded. Stored in Secrets Manager."
}

variable "uc_catalog" {
  type        = string
  default     = "lineage"
  description = "Unity Catalog catalog name for the events table."
}

variable "uc_schema" {
  type        = string
  default     = "lineage"
  description = "Unity Catalog schema name for the events table."
}

variable "uc_table" {
  type        = string
  default     = "events"
  description = "Unity Catalog table name for lineage events."
}

# ----- Auth ------------------------------------------------------------------
variable "lineage_auth_mode" {
  type        = string
  default     = "uc-jwt"
  description = "Lineage auth mode: uc-jwt | static | disabled."
}

variable "lineage_auth_token" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Expected token for static auth mode (e.g. 'Bearer my-token'). Stored in Secrets Manager when set."
}

variable "rust_log" {
  type        = string
  default     = "info"
  description = "RUST_LOG filter for the table-service."
}

# ----- HTTPS / DNS -----------------------------------------------------------
variable "domain_name" {
  type        = string
  default     = ""
  description = "Public FQDN for the lineage endpoint (enables TLS on the NLB when set with hosted_zone_id)."
}

variable "hosted_zone_id" {
  type        = string
  default     = ""
  description = "Route 53 hosted zone id for DNS validation + the alias record."
}

variable "cert_domain_name" {
  type        = string
  default     = ""
  description = "ACM certificate domain (wildcard recommended, e.g. *.example.dev). Defaults to domain_name."
}

# ----- Misc ------------------------------------------------------------------
variable "log_retention_days" {
  type        = number
  default     = 30
  description = "CloudWatch Logs retention."
}

variable "tags" {
  type = map(string)
  default = {
    project   = "open-lineage-connect"
    component = "ecs-lineage"
  }
  description = "Tags applied to all resources."
}
