# =========================
# REDSHIFT SERVERLESS
# =========================

# Namespace — logical container for the warehouse
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-namespace"
  db_name             = "retail_db"
  admin_username      = "admin"
  admin_user_password = var.redshift_password
  
  iam_roles = [aws_iam_role.redshift_role.arn]

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

# Workgroup — compute layer
resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${var.project_name}-workgroup"
  base_capacity  = 8
  
  # Keep private — no public access
  publicly_accessible = true

  subnet_ids         = aws_subnet.redshift[*].id
  security_group_ids = [aws_security_group.redshift.id]

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

# =========================
# VPC FOR REDSHIFT
# =========================
resource "aws_vpc" "redshift" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name        = "${var.project_name}-vpc"
    Project     = var.project_name
  }
}

# Three subnets across availability zones
resource "aws_subnet" "redshift" {
  count             = 3
  vpc_id            = aws_vpc.redshift.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name    = "${var.project_name}-subnet-${count.index}"
    Project = var.project_name
  }
}

# Get available AZs in Sydney
data "aws_availability_zones" "available" {
  state = "available"
}

# Security group — controls who can connect to Redshift
resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift-sg"
  description = "Security group for Redshift Serverless"
  vpc_id      = aws_vpc.redshift.id

  # Allow inbound on Redshift port from within VPC
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all outbound
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = var.project_name
  }
}

# =========================
# IAM ROLE FOR REDSHIFT
# =========================
resource "aws_iam_role" "redshift_role" {
  name = "${var.project_name}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}

# Allow Redshift to read from S3
resource "aws_iam_role_policy" "redshift_s3_policy" {
  name = "${var.project_name}-redshift-s3-policy"
  role = aws_iam_role.redshift_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.project_name}-curated-${var.account_id}",
          "arn:aws:s3:::${var.project_name}-curated-${var.account_id}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartitions"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# =========================
# INTERNET GATEWAY
# =========================
resource "aws_internet_gateway" "redshift" {
  vpc_id = aws_vpc.redshift.id

  tags = {
    Name    = "${var.project_name}-igw"
    Project = var.project_name
  }
}

# Route table — send internet traffic through gateway
resource "aws_route_table" "redshift" {
  vpc_id = aws_vpc.redshift.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.redshift.id
  }

  tags = {
    Name    = "${var.project_name}-rt"
    Project = var.project_name
  }
}

# Associate route table with all subnets
resource "aws_route_table_association" "redshift" {
  count          = 3
  subnet_id      = aws_subnet.redshift[count.index].id
  route_table_id = aws_route_table.redshift.id
}