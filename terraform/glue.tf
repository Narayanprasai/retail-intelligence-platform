# =========================
# GLUE DATABASE
# =========================
resource "aws_glue_catalog_database" "raw" {
  name        = "retail_raw"
  description = "Raw zone — Olist CSV files landed from ingestion"
}

resource "aws_glue_catalog_database" "curated" {
  name        = "retail_curated"
  description = "Curated zone — cleaned Parquet files"
}

# =========================
# CSV CLASSIFIER
# =========================
resource "aws_glue_classifier" "csv_classifier" {
  name = "${var.project_name}-csv-classifier"

  csv_classifier {
    contains_header = "PRESENT"
    delimiter       = ","
    quote_symbol    = "\""
  }
}

# =========================
# IAM ROLE FOR GLUE
# =========================
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach AWS managed Glue policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Allow Glue to read/write S3
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.project_name}-raw-${var.account_id}",
          "arn:aws:s3:::${var.project_name}-raw-${var.account_id}/*",
          "arn:aws:s3:::${var.project_name}-curated-${var.account_id}",
          "arn:aws:s3:::${var.project_name}-curated-${var.account_id}/*",
          "arn:aws:s3:::${var.project_name}-scripts-${var.account_id}",
          "arn:aws:s3:::${var.project_name}-scripts-${var.account_id}/*",
          "arn:aws:s3:::${var.project_name}-logs-${var.account_id}",
          "arn:aws:s3:::${var.project_name}-logs-${var.account_id}/*"
        ]
      }
    ]
  })
}

# =========================
# GLUE CRAWLER — RAW ZONE
# =========================
resource "aws_glue_crawler" "raw_crawler" {
  name          = "${var.project_name}-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.raw.name
  description   = "Crawls raw S3 zone and registers schemas in Glue Catalog"
  classifiers   = [aws_glue_classifier.csv_classifier.name]

  s3_target {
    path = "s3://${var.project_name}-raw-${var.account_id}/olist/"
  }

  schedule = "cron(0 14 * * ? *)"

  schema_change_policy {
    delete_behavior = "DELETE_FROM_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }

configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

# =========================
# GLUE JOB — CSV TO PARQUET
# =========================
resource "aws_glue_job" "csv_to_parquet" {
  name         = "${var.project_name}-csv-to-parquet"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.project_name}-scripts-${var.account_id}/glue_jobs/csv_to_parquet.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--raw_bucket"                       = "${var.project_name}-raw-${var.account_id}"
    "--curated_bucket"                   = "${var.project_name}-curated-${var.account_id}"
    "--database_name"                    = "retail_raw"
    "--TempDir"                          = "s3://${var.project_name}-logs-${var.account_id}/glue-temp/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}