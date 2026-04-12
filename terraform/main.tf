terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket  = "retail-platform-tfstate-013849273657"
    key     = "terraform.tfstate"
    region  = "ap-southeast-2"
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
