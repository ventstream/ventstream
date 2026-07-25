# VPC with public + private subnets across the AZs. The engine, RDS, NATS,
# and OpenSearch all live in private subnets; only the WS ingress LB sits
# public. Single NAT gateway to keep the test cheap (one AZ egress).
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = "${local.name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  enable_nat_gateway   = true
  single_nat_gateway   = true # cost: one NAT for the test (not HA)
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Subnet tags so the AWS Load Balancer Controller can auto-discover where
  # to place public (internet-facing) vs internal load balancers.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}
