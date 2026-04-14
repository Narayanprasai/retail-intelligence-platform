output "raw_bucket_name" {
  description = "S3 raw zone bucket"
  value       = aws_s3_bucket.raw_zone.bucket
}

output "curated_bucket_name" {
  description = "S3 curated zone bucket"
  value       = aws_s3_bucket.curated_zone.bucket
}

output "scripts_bucket_name" {
  description = "S3 scripts bucket"
  value       = aws_s3_bucket.scripts_zone.bucket
}

output "redshift_endpoint" {
  description = "Redshift Serverless endpoint"
  value       = aws_redshiftserverless_workgroup.main.endpoint
}