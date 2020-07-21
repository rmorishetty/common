import os
import json
import boto3
import re
from ade_utils import io_utils


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
LAMBDAS_DIR = os.path.join(CURRENT_DIR, "lambdafunctions")


def replace_variables_config_files(fname):
    reg_ex = re.compile(r"{{(.*?)}}")

    with open(fname, "r") as fd:
        data = fd.read()

    m = re.search(reg_ex, data)
    while m:
        new_val = eval(m.group()[2:-2])
        data = data[:m.start()] + new_val + data[m.end():]
        m = re.search(reg_ex, data)

    return json.loads(data)


lambdas = os.listdir(LAMBDAS_DIR)
lambdas = {l[:-5]: l for l in lambdas if l.endswith(".json")}

for e in lambdas:
    lambdas[e] = replace_variables_config_files(os.path.join(LAMBDAS_DIR, lambdas[e]))
    print(lambdas[e])

lambda_client = boto3.client('lambda')

for l in lambdas:
    try:
        print("Creating Lambda function %s" % l)
        response = lambda_client.create_function(**lambdas[l])
    except Exception as e:
        print("Updating Lambda function %s" %l)
        updated_lambda = {}
        print(lambdas[l].keys())
        print(lambdas[l]['FunctionName'])
        updated_lambda['FunctionName'] = lambdas[l]['FunctionName']
        updated_lambda['S3Bucket'] = lambdas[l]['Code']['S3Bucket']
        updated_lambda['S3Key'] = lambdas[l]['Code']['S3Key']
        updated_lambda['Publish'] = True
        print(updated_lambda)
        response = lambda_client.update_function_code(**updated_lambda)
        print("Updated lambda")
    print("\t", response)

