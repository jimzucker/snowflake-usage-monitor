#To add more information from Snowflake, you must edit the External Function to send more columns, and then 
# Then change the Header column to reflect the new Headers, as well as add the column to this line in handler.
# default = database, forecast, change = row[1:]

import requests
import boto3
import json
import logging

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

SECRET_MANAGER="snowflake_secret_manager"
REGION = "us-east-1"
SLACK_SECRET_KEY_NAME="slack_url"
TEAMS_SECRET_KEY_NAME="teams_url"

def get_secret(secret_name: str, region_name: str) -> str:
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)
    secret = client.get_secret_value(SecretId=secret_name)["SecretString"]
    return secret

def send_slack_message(slack_url: str, message: str):
    message = "```" + message + "```"
    response = requests.post(slack_url, json={"text": message})
    if response.status_code != 200:
        logger.error("Failed to post to Slack. Response: %s", response.text)
    else:
        logger.info("Slack message posted successfully")

def send_teams_message(teams_url: str, message: str):
    message = "```\n" + message + "\n```"
    response = requests.post(teams_url, json={"text": message})
    if response.status_code != 200:
        logger.error("Failed to post to Teams. Response: %s", response.text)
    else:
        logger.info("Teams message posted successfully")

def handler(event, context):
    result = []
    status_code = 200
    try:
        event_body = json.loads(event["body"])
        rows = event_body["data"]
        
        account_width = 19
        forecast_width = 12
        
        for idx, row in enumerate(rows):
            if idx == 0:
                database, forecast, change = row[1:]
                header = f"{'Account'.ljust(account_width)} | {'Forecast*'.ljust(forecast_width)} | {'Change'.strip()}"
                result.append(header)
                divider = "-" * len(header)
                result.append(divider)

            database, forecast, change = row[1:]
            if not database.endswith("(GB)"):
                forecast = f"${forecast}"
            else:
                forecast = str(forecast)
            str_forecast = str(forecast).ljust(forecast_width)
            result.append(f"{database.ljust(account_width)} | {str_forecast} | {change.strip()}")
        
        secret = get_secret(SECRET_MANAGER, REGION)
        result.append("* Forecast components may be ±15-25%")

        slack_url = json.loads(secret)[SLACK_SECRET_KEY_NAME]
        send_slack_message(slack_url, '\n'.join(result))
        
        teams_url = json.loads(secret)[TEAMS_SECRET_KEY_NAME]
        send_teams_message(teams_url, '\n'.join(result))

    except Exception as err:
        status_code = 400

    return {"statusCode": status_code}
