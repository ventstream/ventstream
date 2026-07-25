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

resource "aws_opensearch_domain" "this" {
  domain_name    = local.name
  engine_version = "OpenSearch_2.13"

  cluster_config {
    instance_type  = var.opensearch_instance_type
    instance_count = var.opensearch_instance_count
    # Single-node test: zone awareness + dedicated masters off.
    zone_awareness_enabled = false
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 20
    volume_type = "gp3"
  }

  # Single-node test → place in ONE private subnet (multi-subnet requires
  # zone awareness / multiple nodes).
  vpc_options {
    subnet_ids         = [module.vpc.private_subnets[0]]
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
}
