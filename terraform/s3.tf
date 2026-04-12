# =========================
# RAW ZONE
# =========================
resource "aws_s3_bucket" "raw_zone" {
  bucket = "${var.project_name}-raw-${var.account_id}"

  tags = {
    Environment = "dev"
    Project     = var.project_name
    Zone        = "raw"
  }
}

resource "aws_s3_bucket_versioning" "raw_zone" {
  bucket = aws_s3_bucket.raw_zone.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw_zone" {
  bucket = aws_s3_bucket.raw_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =========================
# CURATED ZONE
# =========================
resource "aws_s3_bucket" "curated_zone" {
  bucket = "${var.project_name}-curated-${var.account_id}"

  tags = {
    Environment = "dev"
    Project     = var.project_name
    Zone        = "curated"
  }
}

resource "aws_s3_bucket_versioning" "curated_zone" {
  bucket = aws_s3_bucket.curated_zone.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "curated_zone" {
  bucket = aws_s3_bucket.curated_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =========================
# FEATURES ZONE
# =========================
resource "aws_s3_bucket" "features_zone" {
  bucket = "${var.project_name}-features-${var.account_id}"

  tags = {
    Environment = "dev"
    Project     = var.project_name
    Zone        = "features"
  }
}

resource "aws_s3_bucket_public_access_block" "features_zone" {
  bucket = aws_s3_bucket.features_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =========================
# SCRIPTS ZONE
# =========================
resource "aws_s3_bucket" "scripts_zone" {
  bucket = "${var.project_name}-scripts-${var.account_id}"

  tags = {
    Environment = "dev"
    Project     = var.project_name
    Zone        = "scripts"
  }
}

resource "aws_s3_bucket_public_access_block" "scripts_zone" {
  bucket = aws_s3_bucket.scripts_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =========================
# LOGS ZONE
# =========================
resource "aws_s3_bucket" "logs_zone" {
  bucket = "${var.project_name}-logs-${var.account_id}"

  tags = {
    Environment = "dev"
    Project     = var.project_name
    Zone        = "logs"
  }
}

resource "aws_s3_bucket_public_access_block" "logs_zone" {
  bucket = aws_s3_bucket.logs_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


