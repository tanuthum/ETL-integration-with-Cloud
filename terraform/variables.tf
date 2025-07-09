variable "aws_region" {
  type = string
  default = "ap-southeast-1"
}

variable "s3_bucket_name" {
  default = "etl-csv-storage-demo"
}

variable "db_username" {
  default = "postgres"
}

variable "db_password" {
  default = "changeme123"
}

variable "db_name" {
  default = "etl_analytics"
}

