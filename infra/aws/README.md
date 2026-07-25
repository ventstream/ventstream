# VentStream on AWS - legacy ephemeral test stack (Terraform)

> **Legacy validation infrastructure:** this Terraform stack still installs the
> deprecated `ventstream-agent` telemetry chart. It is retained to reproduce the
> original AWS scale test and is not a supported standalone or Fleet deployment
> template. Use the maintained Kubernetes guides in `docs-site/deploy/` for new
> installations.

Stands up a **disposable** AWS environment to validate the engine at scale,
then tears it down. Scope (pass 1): **Postgres CDC + WebSockets**. Neo4j is
a later phase (self-hosted `neo4j:5.26-enterprise` in EKS, same as the local
demo).

## What it builds

| Layer | Resource | Notes |
|---|---|---|
| Network | VPC, 3 AZ, public+private subnets, 1 NAT | `network.tf` |
| Registry | ECR repo for the engine image | `ecr.tf` |
| CDC source | RDS PostgreSQL + param group (`rds.logical_replication=1`) | `rds.tf` |
| Sink | Amazon OpenSearch Service, in-VPC, **FGAC basic auth** | `opensearch.tf` |
| Compute | EKS cluster + managed node group + IRSA + addons | `eks.tf` |
| Apps | NATS (JetStream), legacy CDC chart, `ventstream-gateway` (ws) | `helm.tf` |

### Decisions baked in
- **Ephemeral / single-operator** → **local Terraform state** (no S3 backend). `destroy` when done.
- **OpenSearch auth = FGAC master user (basic auth)**. The engine's OpenSearch sink supports basic-auth + API-key only; **SigV4/IAM is not implemented yet** (a TODO in `crates/ventstream-sinks/.../opensearch/mod.rs`). If you want IAM auth, that's an engine change first.
- **CDC and WS are independent pipelines.** CDC = RDS→engine→OpenSearch (no NATS). WS = NATS + gateway + a publisher + SDK clients. Test each separately.
- **CDC runs single-active** (the legacy CDC release is a one-replica StatefulSet because the PostgreSQL replication slot is exclusive).
- **WS gateway** ships the PR #7 connection cap + memory HPA; `ws_max_conns` is wired from Terraform.

## Prerequisites
- Valid AWS creds for the target account (`aws sts get-caller-identity` must succeed). Refresh in-session with `! aws sso login` / `! aws configure` if expired.
- Region: `eu-central-1` (override with `-var region=`).
- Tools: terraform ≥1.4, aws-cli v2, kubectl, helm, docker.

## Apply order (each `terraform apply` here is BILLABLE)

```bash
cd infra/aws
cp terraform.tfvars.example terraform.tfvars   # set/confirm vars
terraform init
terraform apply                                # creates VPC, ECR, RDS, OpenSearch, EKS

# Build + push the engine image to the new ECR repo:
ECR=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin "${ECR%/*}"
docker build -f ../docker/engine.Dockerfile -t "$ECR:latest" ../..
docker push "$ECR:latest"

# Helm releases (NATS + legacy CDC + ws gateway) are applied by helm.tf on the
# same `terraform apply` once the cluster is up; if split out, run them after
# the image is pushed.

aws eks update-kubeconfig --name ventstream --region eu-central-1
kubectl get pods -A
```

## Test

**CDC (Postgres → OpenSearch):**
```bash
# seed + generate load against RDS (psql), then check the OpenSearch domain
# count matches. Index join/FK columns for SQL mode (see concepts/performance).
```

**WebSockets (cap + HPA at scale):**
```bash
# publish to NATS, connect SDK clients through the ALB, ramp past the cap,
# watch /readyz flip + HPA scale (mirrors the minikube validation in PR #7).
```

(Concrete test scripts land with `helm.tf` in the next increment.)

## Tear down
```bash
terraform destroy
```
`force_delete` is set on ECR so images don't block it. Confirm no leftover
EBS volumes / LB ENIs if destroy partially fails.

## Cost

Every apply creates billable EKS, EC2, RDS, OpenSearch, NAT, and load-balancer
resources. Estimate the current cost in the target account and region before
applying, set a budget alert, and destroy the stack immediately after testing.

## Status
The historical stack contains network, ECR, RDS, OpenSearch, EKS, NATS, CDC,
gateway, seed, and test resources. It previously passed `terraform validate`,
but it has not been migrated to canonical engine configuration or Fleet-managed
identity. Revalidate every provider/module version and inspect the full plan
before any new apply. Local state is intentional for this disposable test only.
