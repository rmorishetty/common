variable "region" {
  description = "Region in which the Pipeline will be created."
  default     = "us-west-2"
}

variable "target_id" {
  description = "Target id of the step function/state machine or lambda function that will be triggered."
}

variable "aws_cron_expression" {
  description = "Aws cron expression that will be used to create the rule"
  # example: * 1 * * ? *
}

variable "json_input" {
  description = "Valid JSON text passed to the target."
  default = ""
}

variable "name" {
  description = "name for de cloudwatch rule"
}

variable "target_type" {
  description = "LAMBDA_FUNCTION or STEP_FUNCTION"
}

variable "generic_role" {
  description = "Generic role, should me removed"
}

variable "target_lambda_name" {
  description = "target lambda name"
  default = ""
}

variable "tags" {
  description = "map with tags that will be added to objects created by this module"
  type = "map"
  default = {}
}