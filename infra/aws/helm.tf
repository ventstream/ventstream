# ─── Application layer (runs on EKS) ────────────────────────────────────
# CDC engine = raw Deployment (the ventstream-agent chart is control-plane-
# coupled; the raw form is the proven standalone path from the minikube run).
# WS gateway = the ventstream-gateway Helm chart (cap + memory HPA from PR #7).
# NATS = upstream chart (JetStream on) — only the WS pipeline needs it.

locals {
  ns           = "ventstream"
  engine_image = "${aws_ecr_repository.engine.repository_url}:latest"
  os_endpoint  = "https://${aws_opensearch_domain.this.endpoint}"

  # CDC engine env (simple string values). Secret-backed values (PG/OS
  # passwords) are added as separate env blocks below.
  cdc_env = {
    VS_ROLES                   = "cdc"
    VS_CDC_SOURCE              = "postgres"
    VS_PG_HOST                 = aws_db_instance.pg.address
    VS_PG_PORT                 = "5432"
    VS_PG_USER                 = var.db_username
    VS_PG_DATABASE             = var.db_name
    VS_PG_PUBLICATION          = "ventstream_shop"
    VS_PG_SLOT                 = "vs_cdc_slot"
    VS_PG_BOOTSTRAP_MODE       = "snapshot"
    VS_PG_DENORMALIZE_MODE     = "sql"
    VS_PG_BOOTSTRAP_CHUNK_SIZE = "5000"
    VS_JOINS_YAML              = "/specs/orders.yaml"
    VS_DLQ_PATH                = "/tmp/dlq.jsonl"
    VS_OS_ENDPOINT             = local.os_endpoint
    VS_OS_USERNAME             = var.opensearch_master_user
    VS_INDEX_TEMPLATE          = "orders"
    VS_HEALTH_LISTEN           = "0.0.0.0:4043"
    RUST_LOG                   = "info"
  }
}

resource "kubernetes_namespace_v1" "vs" {
  metadata { name = local.ns }
}

resource "kubernetes_secret_v1" "engine_creds" {
  metadata {
    name      = "engine-creds"
    namespace = local.ns
  }
  data = {
    VS_PG_PASSWORD = random_password.db.result
    VS_OS_PASSWORD = random_password.opensearch.result
  }
}

# Projection spec (reuse the demo's orders.yaml).
resource "kubernetes_config_map_v1" "spec" {
  metadata {
    name      = "vs-spec"
    namespace = local.ns
  }
  data = {
    "orders.yaml" = file("${path.module}/../../demo/stack/specs/orders.yaml")
  }
}

# Seed SQL = demo schema + publication, plus the FK/join indexes SQL mode
# needs. Run once by the seed Job before the CDC engine starts.
resource "kubernetes_config_map_v1" "seed" {
  metadata {
    name      = "vs-seed"
    namespace = local.ns
  }
  data = {
    "seed.sql" = <<-SQL
      ${file("${path.module}/../../demo/stack/seed/postgres.sql")}
      -- SQL-mode join/FK indexes (idempotent):
      CREATE INDEX IF NOT EXISTS idx_oi_order_id     ON shop.order_items(order_id);
      CREATE INDEX IF NOT EXISTS idx_ord_customer_id ON shop.orders(customer_id);
    SQL
  }
}

# One-shot seed against RDS. terraform waits for it to complete, so the CDC
# Deployment (which depends on it) only starts once the publication exists.
resource "kubernetes_job_v1" "seed" {
  metadata {
    name      = "pg-seed"
    namespace = local.ns
  }
  spec {
    backoff_limit = 3
    template {
      metadata { labels = { app = "pg-seed" } }
      spec {
        restart_policy = "Never"
        container {
          name    = "seed"
          image   = "postgres:16"
          command = ["sh", "-c", "psql \"$PGURL\" -v ON_ERROR_STOP=1 -f /seed/seed.sql"]
          env {
            name  = "PGURL"
            value = "postgres://${var.db_username}:${random_password.db.result}@${aws_db_instance.pg.address}:5432/${var.db_name}"
          }
          volume_mount {
            name       = "seed"
            mount_path = "/seed"
          }
        }
        volume {
          name = "seed"
          config_map { name = kubernetes_config_map_v1.seed.metadata[0].name }
        }
      }
    }
  }
  wait_for_completion = true
  timeouts { create = "10m" }
  depends_on = [aws_db_instance.pg, module.eks]
}

# NATS with JetStream — feeds the WS gateway.
resource "helm_release" "nats" {
  name       = "nats"
  namespace  = local.ns
  repository = "https://nats-io.github.io/k8s/helm/charts/"
  chart      = "nats"
  version    = "1.2.2"

  set {
    name  = "config.jetstream.enabled"
    value = "true"
  }
  # Memory JetStream — no PVC, so we don't need the EBS CSI driver for the
  # ephemeral test. The live fan-out consumes deliver_policy=New (no replay),
  # so a memory store is functionally fine; the engine's stream is set to
  # memory storage to match (gateway ws.jetstream.storage below).
  set {
    name  = "config.jetstream.fileStore.enabled"
    value = "false"
  }
  set {
    name  = "config.jetstream.memoryStore.enabled"
    value = "true"
  }
  set {
    name  = "config.jetstream.memoryStore.maxSize"
    value = "1Gi"
  }
  depends_on = [module.eks, kubernetes_namespace_v1.vs]
}

# CDC engine: Postgres → OpenSearch, bounded SQL mode.
resource "kubernetes_deployment_v1" "cdc" {
  metadata {
    name      = "vs-cdc"
    namespace = local.ns
    labels    = { app = "vs-cdc" }
  }
  spec {
    replicas = 1 # single-active: the PG replication slot is exclusive
    selector { match_labels = { app = "vs-cdc" } }
    template {
      metadata { labels = { app = "vs-cdc" } }
      spec {
        container {
          name              = "engine"
          image             = local.engine_image
          image_pull_policy = "Always"

          dynamic "env" {
            for_each = local.cdc_env
            content {
              name  = env.key
              value = env.value
            }
          }
          env {
            name = "VS_PG_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.engine_creds.metadata[0].name
                key  = "VS_PG_PASSWORD"
              }
            }
          }
          env {
            name = "VS_OS_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.engine_creds.metadata[0].name
                key  = "VS_OS_PASSWORD"
              }
            }
          }

          port {
            name           = "health"
            container_port = 4043
          }
          volume_mount {
            name       = "spec"
            mount_path = "/specs"
          }
          readiness_probe {
            http_get {
              path = "/readyz"
              port = "health"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }
          liveness_probe {
            http_get {
              path = "/healthz"
              port = "health"
            }
            initial_delay_seconds = 10
            period_seconds        = 20
          }
          resources {
            requests = { cpu = "200m", memory = "128Mi" }
            limits   = { memory = "512Mi" }
          }
        }
        volume {
          name = "spec"
          config_map { name = kubernetes_config_map_v1.spec.metadata[0].name }
        }
      }
    }
  }
  depends_on = [kubernetes_job_v1.seed, aws_opensearch_domain.this]
}

# WS gateway via the hardened chart (cap + memory HPA). Exposed as an NLB
# (L4, WebSocket-safe) to skip the AWS LB Controller for the test.
resource "helm_release" "gateway" {
  name      = "vs-gw"
  namespace = local.ns
  chart     = "${path.module}/../helm/ventstream-gateway"

  set {
    name  = "image.repository"
    value = aws_ecr_repository.engine.repository_url
  }
  set {
    name  = "image.tag"
    value = "latest"
  }
  set {
    name  = "image.pullPolicy"
    value = "Always"
  }
  set {
    name  = "roles"
    value = "ws"
  }
  set {
    name  = "nats.url"
    value = "nats://nats.${local.ns}.svc:4222"
  }
  set {
    name  = "ws.maxConns"
    value = var.ws_max_conns
  }
  set {
    # Match NATS memory JetStream — the ws role bootstraps the stream with
    # this storage class, so it must agree with the NATS store above.
    name  = "ws.jetstream.storage"
    value = "memory"
  }
  set {
    name  = "autoscaling.enabled"
    value = "true"
  }
  set {
    name  = "service.type"
    value = "LoadBalancer"
  }

  depends_on = [helm_release.nats, helm_release.metrics_server]
}
