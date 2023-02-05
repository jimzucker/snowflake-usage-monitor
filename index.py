import requests
import boto3
import json

def get_secret(secret_name: str, region_name: str) -> str:
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)
    try:
        secret = client.get_secret_value(SecretId=secret_name)["SecretString"]
        return secret
    except client.exceptions.ResourceNotFoundException as e:
        print(f"Error: The requested secret '{secret_name}' was not found in Secrets Manager")
        raise e

def send_slack_message(webhook_url: str, message: str):
    response = requests.post(webhook_url, json={"text": message})
    if response.status_code != 200:
        print("Failed to post to Slack. Response:", response.text)
    else:
        print("Slack message posted successfully")
  
def send_teams_message(teams_url: str, teams_message: str):
    response = requests.post(teams_url, json={"text": teams_message})
    if response.status_code != 200:
        print("Failed to post to Teams. Response:", response.text)
    else:
        print("Teams message posted successfully")

def handler(event, context):
    result = []
    status_code = 200
    try:
        event_body = json.loads(event["body"])
        rows = event_body["data"]

        for row in rows:
            database, forecast, change = row[1:]
            result.append(f"*{database}* - Forecast: {forecast} ({change.strip()})")
        
        secret = get_secret("slackurl", "us-east-1")
        secret2 = get_secret("teamsurl", "us-east-1")
        webhook_url = json.loads(secret)["slack_url"]
        teams_url = json.loads(secret)["teams_url"]
        send_slack_message(webhook_url, '\n'.join(result))
        send_teams_message(tea_url, '\n'.join(result))


    except Exception as err:
        status_code = 400

    return {
        "statusCode": status_code,
        "body": {"message": "Success"}
    }