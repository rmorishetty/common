resource "aws_codebuild_project" "proyect" {
  name          = "${var.name}"
  description   = "${var.description}"
  build_timeout = "${var.build_timeout}"
  queued_timeout = "${var.queued_timeout}"
  service_role  = "${var.service_role}"
  
  source {
    type = "NO_SOURCE"
    buildspec = "${var.buildspec}"
  }

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    compute_type = "${var.compute_type}" 
    image =  "${var.image}" 
    type = "${var.container_os_type}" 
    image_pull_credentials_type = "CODEBUILD"
    environment_variable {
      name  = "TERRAFORM_USER"
      value = "${var.terraform_codecommit_user}" 
      type = "PLAINTEXT"
    }
    environment_variable {
      name  = "TERRAFORM_PASS"
      value = "${var.terraform_codecommit_pass}"
      type = "PLAINTEXT"
    }
    environment_variable {
      name  = "ENV"
      value = "${var.env}" 
      type = "PLAINTEXT"
    }
  }

}
