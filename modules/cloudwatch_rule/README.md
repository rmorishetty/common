## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| aws\_cron\_expression | Aws cron expression that will be used to create the rule | string | n/a | yes |
| generic\_role | Generic role, should me removed | string | n/ | no |
| name | name for de cloudwatch rule | string | n/a | yes |
| region | Region in which the Pipeline will be created. | string | `"us-west-2"` | no |
| target\_id | Target id of the step function/state machine or lambda function that will be triggered. | string | n/a | yes |
| target\_type | LAMBDA\_FUNCTION or STEP\_FUNCTION | string | n/a | yes |

