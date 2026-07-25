# Registry for the engine image. Build locally from infra/docker/engine.Dockerfile
# and push here (see README) — EKS nodes pull via the node role's ECR perms.
resource "aws_ecr_repository" "engine" {
  name                 = "${local.name}-engine"
  image_tag_mutability = "MUTABLE"
  force_delete         = true # ephemeral stack: let destroy clean it even with images

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "engine" {
  repository = aws_ecr_repository.engine.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 10 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}
