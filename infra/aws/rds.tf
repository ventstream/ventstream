# --- RDS PostgreSQL: the CDC source ---

resource "aws_db_subnet_group" "pg" {
  name       = "${local.name}-pg"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "rds" {
  name_prefix = "${local.name}-rds-"
  vpc_id      = module.vpc.vpc_id

  # Postgres from inside the VPC only (EKS nodes live here). No public access.
  ingress {
    from_port   = 5432
    to_port     = 5432
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

# Logical replication is what the CDC engine needs. `rds.logical_replication`
# is a STATIC parameter — it flips wal_level=logical and requires the reboot
# that creating the instance with this group provides.
resource "aws_db_parameter_group" "pg" {
  name_prefix = "${local.name}-pg16-"
  family      = "postgres16"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  # The engine connects to Postgres with NoTls (no PG-TLS support yet), but
  # RDS defaults to requiring encrypted connections (pg_hba rejects
  # "no encryption"). Allow unencrypted — fine in-VPC for this test; the
  # proper prod fix is adding rustls TLS to the engine's PG connection.
  parameter {
    name         = "rds.force_ssl"
    value        = "0"
    apply_method = "immediate"
  }
  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_wal_senders"
    value        = "10"
    apply_method = "pending-reboot"
  }
  lifecycle { create_before_destroy = true }
}

resource "aws_db_instance" "pg" {
  identifier     = "${local.name}-pg"
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  allocated_storage = var.db_allocated_storage
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.db_username
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.pg.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.pg.name

  publicly_accessible = false
  multi_az            = false # test: single-AZ

  # Ephemeral: don't block destroy on a final snapshot.
  skip_final_snapshot = true
  deletion_protection = false

  apply_immediately = true
}
