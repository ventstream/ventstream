# --- EKS cluster + managed node group ---
# The managed node role gets AmazonEC2ContainerRegistryReadOnly by default,
# so nodes can pull the engine image from ECR with no extra wiring.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"

  cluster_name    = local.name
  cluster_version = var.kubernetes_version

  # Public endpoint so we can kubectl/helm from this machine for the test.
  cluster_endpoint_public_access = true
  # Grant the applying principal cluster-admin (v20 access entries) so
  # kubectl + the helm/kubernetes providers work right after apply.
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    default = {
      instance_types = [var.node_instance_type]
      # ARM64 AMI to match the Graviton instance type (and the arm64 image).
      ami_type     = "AL2023_ARM_64_STANDARD"
      min_size     = var.node_min_size
      max_size     = var.node_max_size
      desired_size = var.node_desired_size
    }
  }
}

# metrics-server: required for the WS gateway's memory-based HPA (EKS does
# not ship it). No IRSA needed.
resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  namespace  = "kube-system"
  version    = "3.12.1"

  set {
    name  = "args[0]"
    value = "--kubelet-preferred-address-types=InternalIP\\,Hostname\\,ExternalIP"
  }

  depends_on = [module.eks]
}
