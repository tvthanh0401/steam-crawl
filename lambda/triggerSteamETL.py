import json
import boto3


def lambda_handler(event, context):
    client = boto3.client("glue")


    client.start_job_run(
        JobName="Processing_Data_Steam_Game",
        Arguments={"--bucket_name": 'steametl'}
    )

    return {"statusCode": 200, "body": json.dumps("steametl triggered")}