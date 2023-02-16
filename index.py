import requests
import boto3
import json
import logging
import datetime

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

SECRET_MANAGER="snowflake_secret_manager"
REGION = "us-east-1"
SLACK_SECRET_KEY_NAME="slack_url"
TEAMS_SECRET_KEY_NAME="teams_url"


def get_secret(secret_name: str, region_name: str) -> str:
	session = boto3.session.Session()
	client = session.client("secretsmanager", region_name=region_name)
	try:
		secret = client.get_secret_value(SecretId=secret_name)["SecretString"]
	except Exception as e:
		if e.response['Error']['Code'] == 'InvalidRequestException':
			logger.error("The request was invalid due to:", e)
		elif e.response['Error']['Code'] == 'InvalidParameterException':
			logger.error("The request had invalid params:", e)
			
	return secret

def send_slack_message(slack_url: str, message: str):
	if slack_url == "":
		return
	slack_message = {
		'text': message
	}
 
	response = requests.post(slack_url, json.dumps(slack_message).encode('utf-8'))
	if response.status_code != 200:
		print("Failed to post to Slack. Response:", response.text)
	else:
		print("Slack message posted successfully")
	if response.status_code != 200:
		print("Failed to post to Slack. Response:", response.text)
	else:
		print("Slack message posted successfully")
	  
def send_teams_message(teams_url: str, message: str):
	if teams_url == "":
		return

	teams_message = {
		'text': message
	}
	response = requests.post(teams_url, json.dumps(teams_message).encode('utf-8'))
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
		result.append("```\n")
  
		for row in rows:
			database, forecast, change = row[1:]
			database = database.ljust(16)
			forecast = forecast.ljust(10)
			change = change.strip(8)
			result.append(f"{database} | {forecast} | ({change})\n")
		result.append("```")

		secret = get_secret(SECRET_MANAGER, REGION)
		
		slack_url = json.loads(secret)[SLACK_SECRET_KEY_NAME]
		send_slack_message(slack_url, '\n'.join(result))
		
		teams_url = json.loads(secret)[TEAMS_SECRET_KEY_NAME]
		send_teams_message(teams_url, '\n'.join(result))
	except Exception as err:
		status_code = 400

	return {"statusCode": status_code}