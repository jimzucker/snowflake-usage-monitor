import json
import requests

def handler(event, context):
    status_code = 200
    result = []
    SLACK_URL = "https://hooks.slack.com/services/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"

    try:
        event_body = json.loads(event["body"])
        rows = event_body["data"]

        for row in rows:
            database, mtd, forecast, prior_month, change = row[1:]
            result.append(f"{database} - MTD:{mtd} Forecast:{forecast} Prior Month:{prior_month} Change:{change.strip()}")

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