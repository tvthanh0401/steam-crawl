import boto3
import aws_cred
import datetime
import time

session = boto3.Session(
    aws_access_key_id=aws_cred.ID,
    aws_secret_access_key=aws_cred.ACCESS_KEY,
)
s3 = session.resource('s3')
# Filename - File to upload
# Bucket - Bucket to upload to (the top level directory under AWS S3)
# Key - S3 object name (can contain subdirectories). If not specified then file_name is used

# Upload a file to S3
s3.meta.client.upload_file(Filename='link.json', Bucket='steametl', Key='link.json')
s3.meta.client.upload_file(Filename='deal.json', Bucket='steametl', Key='deal.json')
s3.meta.client.upload_file(Filename='reviews.json', Bucket='steametl', Key='reviews.json')
