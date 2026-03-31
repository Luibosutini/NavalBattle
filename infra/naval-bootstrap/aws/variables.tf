variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

variable "name_prefix" {
  description = "Resource name prefix (lowercase letters, digits, hyphen)"
  type        = string
  default     = "naval-vnext"
}

variable "force_destroy" {
  description = "Allow deleting non-empty S3 bucket on destroy"
  type        = bool
  default     = false
}
