# Generated credentials. For the ephemeral test these flow straight into
# the RDS/OpenSearch resources and into k8s secrets the engine reads. (A
# persistent env would park these in AWS Secrets Manager instead.)
resource "random_password" "db" {
  length  = 24
  special = false # keep it psql-connection-string friendly
}

# OpenSearch FGAC master password must satisfy the domain's password policy:
# >= 8 chars with upper, lower, digit, and a special char.
resource "random_password" "opensearch" {
  length           = 20
  special          = true
  override_special = "!#$%"
  min_upper        = 2
  min_lower        = 2
  min_numeric      = 2
  min_special      = 2
}
