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

variable "opensearch_topology" {
  description = "acceptance keeps the disposable single-node domain; ha provisions a three-AZ, standby-enabled production topology."
  type        = string
  default     = "acceptance"

  validation {
    condition     = contains(["acceptance", "ha"], var.opensearch_topology)
    error_message = "opensearch_topology must be acceptance or ha."
  }
}

variable "opensearch_ha_instance_type" {
  description = "Data node type used by the HA topology."
  type        = string
  default     = "r6g.large.search"
}

variable "opensearch_ha_instance_count" {
  description = "Data nodes used by the HA topology. Multi-AZ with Standby requires a multiple of three."
  type        = number
  default     = 3

  validation {
    condition     = var.opensearch_ha_instance_count >= 3 && var.opensearch_ha_instance_count % 3 == 0
    error_message = "opensearch_ha_instance_count must be at least 3 and divisible by 3."
  }
}

variable "opensearch_ha_master_instance_type" {
  description = "Dedicated cluster-manager node type used by the HA topology."
  type        = string
  default     = "m6g.large.search"
}

variable "opensearch_alarm_actions" {
  description = "SNS topic ARNs notified by OpenSearch availability/capacity alarms."
  type        = list(string)
  default     = []
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
