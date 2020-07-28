output "name" {
  value       = "${var.name}"
  description = "name of the created glue endpoint"
}

#output "target_arn" {
#  value       = "${aws_dms_endpoint.s3_parquet_target.*.endpoint_arn}"
#  description = "S3 endpoint target arn."
#}
#
#output "source_arn" {
#  value       = "${aws_dms_endpoint.aurora_source.*.endpoint_arn}"
#  description = "Aurora endpoint source arn."
#}

output "endpoint_arn" {
  value       = "${element(concat(aws_dms_endpoint.aurora_source.*.endpoint_arn, aws_dms_endpoint.s3_parquet_target.*.endpoint_arn), 0)}"
  description = "endpoint target arn."
}
