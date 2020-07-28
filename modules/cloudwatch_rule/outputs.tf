output "id" {
  value       = "${aws_cloudwatch_event_rule.cloudwatch_event_rule.arn}"
  description = "cloudwatch rule arn"
}
output "name" {
  value       = "${var.name}"
  description = "cloudwatch rule name"
}