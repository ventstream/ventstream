output "region" {
  value = var.region
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "private_subnets" {
  value = module.vpc.private_subnets
}

output "ecr_repository_url" {
  description = "Push the engine image here before the apps can pull it."
  value       = aws_ecr_repository.engine.repository_url
}

output "rds_endpoint" {
  value = aws_db_instance.pg.address
}

output "opensearch_endpoint" {
  description = "In-VPC HTTPS endpoint the CDC engine sinks to (reachable from EKS pods only)."
  value       = aws_opensearch_domain.this.endpoint
}

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "ws_loadbalancer_command" {
  description = "Run after apply to get the WS gateway's public NLB hostname."
  value       = "kubectl -n ventstream get svc -l app.kubernetes.io/name=ventstream-gateway -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}'"
}

output "db_password" {
  value     = random_password.db.result
  sensitive = true
}

output "opensearch_password" {
  value     = random_password.opensearch.result
  sensitive = true
}
