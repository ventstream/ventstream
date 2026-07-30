# --- Amazon OpenSearch Service: the sink ---
# In-VPC, TLS-enforced, with Fine-Grained Access Control (FGAC) so the engine
# can use a basic-auth master user — the sink supports basic-auth/API-key,
# NOT SigV4/IAM (yet). FGAC requires encryption-at-rest + node-to-node
# encryption + HTTPS, all set below.

resource "aws_security_group" "opensearch" {
  name_prefix = "${local.name}-os-"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { create_before_destroy = true }
}

data "aws_caller_identity" "current" {}

locals {
  opensearch_ha              = var.opensearch_topology == "ha"
  opensearch_instance_type   = local.opensearch_ha ? var.opensearch_ha_instance_type : var.opensearch_instance_type
  opensearch_instance_count  = local.opensearch_ha ? var.opensearch_ha_instance_count : var.opensearch_instance_count
  opensearch_ebs_volume_size = local.opensearch_ha ? 100 : 20
  opensearch_subnet_ids      = slice(module.vpc.private_subnets, 0, local.opensearch_ha ? 3 : 1)
}

resource "aws_opensearch_domain" "this" {
  domain_name    = local.name
  engine_version = "OpenSearch_2.13"

  cluster_config {
    instance_type                 = local.opensearch_instance_type
    instance_count                = local.opensearch_instance_count
    zone_awareness_enabled        = local.opensearch_ha
    multi_az_with_standby_enabled = local.opensearch_ha
    dedicated_master_enabled      = local.opensearch_ha
    dedicated_master_type         = local.opensearch_ha ? var.opensearch_ha_master_instance_type : null
    dedicated_master_count        = local.opensearch_ha ? 3 : null

    dynamic "zone_awareness_config" {
      for_each = local.opensearch_ha ? [1] : []
      content {
        availability_zone_count = 3
      }
    }
  }

  ebs_options {
    ebs_enabled = true
    volume_size = local.opensearch_ebs_volume_size
    volume_type = "gp3"
  }

  vpc_options {
    subnet_ids         = local.opensearch_subnet_ids
    security_group_ids = [aws_security_group.opensearch.id]
  }

  encrypt_at_rest { enabled = true }
  node_to_node_encryption { enabled = true }
  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  advanced_security_options {
    enabled                        = true
    internal_user_database_enabled = true
    master_user_options {
      master_user_name     = var.opensearch_master_user
      master_user_password = random_password.opensearch.result
    }
  }

  software_update_options {
    auto_software_update_enabled = true
  }

  auto_tune_options {
    desired_state = local.opensearch_ha ? "ENABLED" : "DISABLED"
  }

  # In-VPC + FGAC: the domain access policy can be open; FGAC enforces authz.
  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "es:*"
      Resource  = "arn:aws:es:${var.region}:*:domain/${local.name}/*"
    }]
  })

  lifecycle {
    precondition {
      condition     = !local.opensearch_ha || var.az_count >= 3
      error_message = "The OpenSearch HA topology requires at least three VPC availability zones."
    }
  }
}

locals {
  opensearch_alarm_dimensions = {
    ClientId   = data.aws_caller_identity.current.account_id
    DomainName = aws_opensearch_domain.this.domain_name
  }
}

resource "aws_cloudwatch_metric_alarm" "opensearch_cluster_red" {
  count = local.opensearch_ha ? 1 : 0

  alarm_name          = "${local.name}-opensearch-cluster-red"
  alarm_description   = "OpenSearch has at least one unassigned primary shard."
  namespace           = "AWS/ES"
  metric_name         = "ClusterStatus.red"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "breaching"
  dimensions          = local.opensearch_alarm_dimensions
  alarm_actions       = var.opensearch_alarm_actions
  ok_actions          = var.opensearch_alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "opensearch_free_storage" {
  count = local.opensearch_ha ? 1 : 0

  alarm_name          = "${local.name}-opensearch-free-storage"
  alarm_description   = "OpenSearch free storage is below 20 GiB on a data node."
  namespace           = "AWS/ES"
  metric_name         = "FreeStorageSpace"
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 20480
  comparison_operator = "LessThanOrEqualToThreshold"
  treat_missing_data  = "breaching"
  dimensions          = local.opensearch_alarm_dimensions
  alarm_actions       = var.opensearch_alarm_actions
  ok_actions          = var.opensearch_alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "opensearch_jvm_pressure" {
  count = local.opensearch_ha ? 1 : 0

  alarm_name          = "${local.name}-opensearch-jvm-pressure"
  alarm_description   = "OpenSearch JVM memory pressure remained at or above 95 percent."
  namespace           = "AWS/ES"
  metric_name         = "JVMMemoryPressure"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 95
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "breaching"
  dimensions          = local.opensearch_alarm_dimensions
  alarm_actions       = var.opensearch_alarm_actions
  ok_actions          = var.opensearch_alarm_actions
}
