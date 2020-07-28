resource "aws_dms_endpoint" "aurora_source" {
  count = "${var.type == "source" ? 1 : 0}"
  endpoint_id = "${var.name}"
  engine_name = "aurora"
  endpoint_type = "source"
  username = "${var.aurora_username}"
  kms_key_arn = "${var.aurora_kms_key_arn}"
  server_name = "${var.aurora_server_name}"
  password = "${var.aurora_password}"
  port = "${var.aurora_port}"
  ssl_mode = "${var.ssl_mode}"

  tags = {
    subject = "${var.subject}"
  }

}

resource "aws_dms_endpoint" "s3_parquet_target" {
  count = "${var.type == "target" ? 1 : 0}"
  endpoint_id                 = "${var.name}"
  endpoint_type               = "target"
  engine_name                 = "s3"
  extra_connection_attributes = "bucketFolder=${var.s3_bucket_folder};bucketName=${var.s3_bucket_name};compressionType=NONE;csvDelimiter=,;csvRowDelimiter=\\n;"
  ssl_mode                    = "${var.ssl_mode}"
  service_access_role         = "${var.s3_service_access_role}"

  s3_settings = {
      service_access_role_arn = "${var.s3_service_access_role}"
      external_table_definition = "string"
      bucket_folder = "${var.s3_bucket_folder}"
      bucket_name = "${var.s3_bucket_name}"
  }

  tags = {
    subject = "${var.subject}"
  }

}
  
resource "null_resource" "mod-endpoint" {
  count = "${var.type == "target" ? 1 : 0}"
  provisioner "local-exec" {
    command = "aws dms modify-endpoint --endpoint-arn ${aws_dms_endpoint.s3_parquet_target.endpoint_arn} --s3-settings ExternalTableDefinition=\"string\",BucketFolder=\"${var.s3_bucket_folder}\",BucketName=\"${var.s3_bucket_name}\",DataFormat=\"parquet\",EnableStatistics=true,IncludeOpForFullLoad=true,TimestampColumnName=\"TIMESTAMP\",ParquetTimestampInMillisecond=true"
  }
  depends_on = ["aws_dms_endpoint.s3_parquet_target"]
}


