import sys
import logging
import requests
import boto3
import os
import datetime
from dateutil.relativedelta import relativedelta
from botocore.exceptions import ClientError
import json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from base64 import b64decode


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

AWSGENIE_SECRET_MANAGER="awsgenie_secret_manager"
SLACK_SECRET_KEY_NAME="slack_url"
SNS_SECRET_KEY_NAME="sns_arn"

def get_secret(sm_client,secret_key_name):
    # if AWS_LAMBDA_FUNCTION_NAME == "":
    try:
        text_secret_data = ""
        get_secret_value_response = sm_client.get_secret_value( SecretId=AWSGENIE_SECRET_MANAGER )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("The request had invalid params:", e)

    # Secrets Manager decrypts the secret value using the associated KMS CMK
    # Depending on whether the secret was a string or binary, only one of these fields will be populated
    if 'SecretString' in get_secret_value_response:
        text_secret_data = json.loads(get_secret_value_response['SecretString']).get(secret_key_name)
    else:
        #binary_secret_data = get_secret_value_response['SecretBinary']
        logger.error("Binary Secrets not supported")

        # Your code goes here.
    return text_secret_data
    # else:
    #     return ""

def send_slack(slack_url, message):
    
    #make it a NOP if URL is NULL
    if slack_url == "":
        return

    slack_message = {
        'text': message
    }

    req = Request(slack_url, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.debug("Message posted to slack")
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
        logger.error("SLACK_URL= %s", slack_url)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)
        logger.error("slack_url= %s", slack_url)

def display_output(boto3_session, message):
    secrets_manager_client = boto3_session.client('secretsmanager')
    try:
        slack_url='https://' + get_secret(secrets_manager_client, SLACK_SECRET_KEY_NAME)
        send_slack(slack_url, message)
    except Exception as e:
        logger.info("Disabling Slack, URL not found")


def handler(event, context):
    status_code = 200
    result = []
    SLACK_URL = "<SLACK URL HERE>"

    try:
        event_body = json.loads(event["body"])
        rows = event_body["data"]

        for row in rows:
            database, forecast, change = row[1:]
            result.append(f"*{database}*  -  Forecast : {forecast} ({change.strip()})")

        slack_message = {"text": '\n'.join(result)}
        response = requests.post(SLACK_URL, json=slack_message)

        if response.status_code != 200:
            print("Failed to post to Slack. Response:", response.text)
        else:
            print("Slack message posted successfully")

    except Exception as err:
        status_code = 400
        result = event_body

    return {
        "statusCode": status_code,
        "body": json.dumps(result)
    }