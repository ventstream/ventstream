provider "aws" {
  region = var.region
  default_tags {
    tags = var.tags
  }
}

provider "random" {}

# The kubernetes + helm providers authenticate against the EKS cluster
# created in eks.tf. They're configured here but only exercised once the
# cluster exists (helm.tf). Token via the AWS CLI exec plugin so it stays
# fresh across long applies.
provider "kubernetes" {
  host                   = try(module.eks.cluster_endpoint, "")
  cluster_ca_certificate = try(base64decode(module.eks.cluster_certificate_authority_data), "")
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", try(module.eks.cluster_name, ""), "--region", var.region]
  }
}

provider "helm" {
  kubernetes {
    host                   = try(module.eks.cluster_endpoint, "")
    cluster_ca_certificate = try(base64decode(module.eks.cluster_certificate_authority_data), "")
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", try(module.eks.cluster_name, ""), "--region", var.region]
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs  = slice(data.aws_availability_zones.available.names, 0, var.az_count)
  name = var.project
}
