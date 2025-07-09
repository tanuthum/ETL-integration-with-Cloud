provider "aws" {
  region  = var.aws_region
  profile = "default"
}

# S3 Bucket for CSV storage
resource "aws_s3_bucket" "etl_bucket" {
  bucket = var.s3_bucket_name
}

# RDS PostgreSQL Instance
resource "aws_db_instance" "postgres" {
  identifier              = "etl-postgres-db"
  engine                  = "postgres"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  username                = var.db_username
  password                = var.db_password
  db_name                 = var.db_name
  skip_final_snapshot     = true
  publicly_accessible     = true
}

# Security Group for RDS PostgreSQL (open to all IPs — use with caution in dev)
resource "aws_security_group" "rds_sg" {
  name        = "rds-security-group"
  description = "Allow PostgreSQL access from anywhere"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["49.228.106.182/32"] 
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Outputs
output "rds_endpoint" {
  value = aws_db_instance.postgres.endpoint
}

output "s3_bucket_name" {
  value = aws_s3_bucket.etl_bucket.bucket
}

output "db_name" {
  value = var.db_name
}

output "db_user" {
  value = var.db_username
}