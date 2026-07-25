variable "project" {
  description = "Name prefix for all resources + tags."
  type        = string
  default     = "ventstream"
}

variable "region" {
  description = "AWS region. Defaults to the operator's configured region."
  type        = string
  default     = "eu-central-1"
}

variable "vpc_cidr" {
  description = "CIDR for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "az_count" {
  description = "Number of AZs to spread subnets across."
  type        = number
  default     = 3
}

# --- RDS Postgres (CDC source) ---
variable "db_instance_class" {
  description = "RDS instance class. db.t3.medium is plenty for the test."
  type        = string
  default     = "db.t3.medium"
}

variable "db_name" {
  type    = string
  default = "shop"
}

variable "db_username" {
  type    = string
  default = "ventstream"
}

variable "db_allocated_storage" {
  type    = number
  default = 20
}

# --- OpenSearch (sink) ---
variable "opensearch_instance_type" {
  description = "OpenSearch data node type. t3.small.search is fine for the test."
  type        = string
  default     = "t3.small.search"
}

variable "opensearch_instance_count" {
  type    = number
  default = 1
}

variable "opensearch_master_user" {
  description = "FGAC master user (basic auth). The engine sink uses this — SigV4/IAM is not yet supported by the sink."
  type        = string
  default     = "vsadmin"
}

# --- EKS ---
variable "kubernetes_version" {
  type    = string
  default = "1.30"
}

variable "node_instance_type" {
  description = "Managed node group instance type. Graviton (arm64) so the locally-built arm64 engine image runs natively without a cross-platform rebuild."
  type        = string
  default     = "m6g.large"
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 5
}

# --- WS gateway tuning (carried into the Helm release) ---
variable "ws_max_conns" {
  description = "Per-pod WS connection cap (the OOM backstop from PR #7). Size to the node/pod memory."
  type        = number
  default     = 5000
}

variable "tags" {
  description = "Extra tags applied to everything."
  type        = map(string)
  default = {
    Project   = "ventstream"
    Env       = "test-ephemeral"
    ManagedBy = "terraform"
  }
}
