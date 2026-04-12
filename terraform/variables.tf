variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "aws_profile" {
  description = "AWS CLI profile"
  type        = string
  default     = "default"
}

variable "project_name" {
  description = "Project name for naming resources"
  type        = string
  default     = "retail-platform"
}

variable "account_id" {
  description = "AWS account ID"
  type        = string
  default     = "013849273657"
}
