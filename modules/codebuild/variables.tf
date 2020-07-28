variable "name" {
  description = "name for the code build project"
}
variable "description" {
  description = "description for the code build project"
  default = ""
}
variable "build_timeout" {
  description = "How long in minutes, from 5 to 480 (8 hours), for AWS CodeBuild to wait until timing out any related build that does not get marked as completed. The default is 60 minutes."
  default = "60"
}
variable "queued_timeout" {
  description = "How long in minutes, from 5 to 480 (8 hours), a build is allowed to be queued before it times out. The default is 8 hours."
  default = "240"
}
variable "service_role" {
  description = "(Required) The Amazon Resource Name (ARN) of the AWS Identity and Access Management (IAM) role that enables AWS CodeBuild to interact with dependent AWS services on behalf of the AWS account."
}
variable "buildspec" {
  description = "The build spec declaration to use for this build project's related builds"
  default = ""
}
variable "compute_type" {
  description = "nformation about the compute resources the build project will use. Available values for this parameter are: BUILD_GENERAL1_SMALL, BUILD_GENERAL1_MEDIUM, BUILD_GENERAL1_LARGE or BUILD_GENERAL1_2XLARGE."
  default = "BUILD_GENERAL1_SMALL"
}
variable "image" {
  description = "The Docker image to use for this build project. Valid values include Docker images provided by CodeBuild"
  default = "aws/codebuild/amazonlinux2-x86_64-standard:2.0"
}
variable "container_os_type" {
  description = "The type of build environment to use for related builds."
  default = "LINUX_CONTAINER"
}
variable "terraform_codecommit_user" {
  description = "Terraform user to clone code from code commit"
}
variable "terraform_codecommit_pass" {
  description = "Terraform password to clone code from code commit"
}
variable "env" {
  description = "Environment for the buildspec to be created"
}