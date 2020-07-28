#cloudwatch rule
resource "aws_cloudwatch_event_rule" "cloudwatch_event_rule" {
  name        = "${var.name}"
  schedule_expression = "cron(${var.aws_cron_expression})"
  tags = "${var.tags}"
}


resource "aws_cloudwatch_event_target" "lambda_target" {
  count = "${var.target_type == "LAMBDA_FUNCTION" ? 1 : 0}"
  rule      = "${aws_cloudwatch_event_rule.cloudwatch_event_rule.id}"
  arn       = "${var.target_id}"
  input = "${var.json_input}"
}

resource "aws_cloudwatch_event_target" "sfn_target" {
  count = "${var.target_type == "STEP_FUNCTION" ? 1 : 0}"
  rule      = "${aws_cloudwatch_event_rule.cloudwatch_event_rule.id}"
  arn       = "${var.target_id}"
  role_arn  = "${var.generic_role}"
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call" {
    count = "${var.target_type == "LAMBDA_FUNCTION" ? 1 : 0}"
    statement_id = "AllowExecutionFromCloudWatch_${var.name}"
    action = "lambda:InvokeFunction"
    function_name = "${var.target_lambda_name}"
    principal = "events.amazonaws.com"
    source_arn = "${aws_cloudwatch_event_rule.cloudwatch_event_rule.arn}"
}