# Snowflake Usage Monitoring
## User Story
As a Developer I want a daily usage update that shows how my compute and storage usage is trending to the prior month.

The goal is for this to be similiar to the data we get from AWS Cost Explorer.  

As the Snowflake Views do not seem to have cost info we will show the data in capacity units that it is quoted in,
The METERING_HISTORY for compute does not seem to have a forecast so we will create a simple one based on MTD average where 
	FORECAST = MTD + RemainingDays in month * AVG(MTD Daily Usage)

We will support 2 options in configutaration for output to slack

### Format 1: Just one line for an Account

Account | Forecast |  Trend<BR>
12345   | 9,999.9  |  +/-XX%

### Format 2: One row for each Warehouse
accountId-1234    | Forecast |  Trend<BR>
WAREHOUSE1     | 9,999.9  |  +/-XX%<BR>
WAREHOUSE2     | 9,999.9  |  +/-XX%<BR>
WAREHOUSE3     | 9,999.9  |  +/-XX%<BR>


## Acceptance Criteria
1. Instructions provided in README to install
2. Both formats work on Slack using Snowflake External Function Feature


# Installation Instructions

## Snowflake View Installation Iinstructions
1. Run the sql `METERING_HISTORY_TREND.sql` to create the USAGE_MONITOR database and CALC_METERING_HISTORY_TREND stored procedure.

2. Execute the stored procedure to populate the view METERING_HISTORY_TREND
```
USE DATABASE USAGE_MONITOR; 
call CALC_METERING_HISTORY_TREND();
```

3. Query the view to see the current statistics: 
```
SELECT * from METERING_HISTORY_TREND;
```
![Image of Cost Explorer](https://github.com/jimzucker/snowflake-usage-monitor/blob/main/images/METRIC_HISTORY_TREND.png)

```
SELECT * from METERING_HISTORY_NAME_TREND;
```
![Image of Cost Explorer](https://github.com/jimzucker/snowflake-usage-monitor/blob/main/images/METRIC_HISTORY_NAME_TREND.png)





## AWS Slack External Function Installation Instructions
Steps for creating Snowflake external function using the CloudFormation template:

1. Go to AWS cloudformation and create a stack using this template:
```
snowflake-usage-monitor-cf.yaml
```
2. Note the Gateway IAM role and URL of the "slack_post" method created in the API Gateway.
3. Create API integration in Snowflake using the Gatway URL and Gateway Role ARN

```
/*
    Update API_AWS_ROLE_ARN & API_ALLOWED_PREFIXES to output of running your cloud formation stack
*/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION  usage_monitor_slack_integration
    API_PROVIDER = aws_api_gateway
--    API_PROVIDER = aws_private_api_gateway 
    API_AWS_ROLE_ARN = 'arn:aws:iam::<id>:role/snowflake-usage-monitor-agw-role'
    API_ALLOWED_PREFIXES = ('https://<id>.execute-api.us-east-1.amazonaws.com/snowflake-usage-monitor-stage/slack_post')
    ENABLED = TRUE
    COMMENT = 'Post Usage Monitoring data to Slack'
    ;

-- Update the API Gateway role trust relation with API integration's API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION usage_monitor_slack_integration;
```

4. Update the API Gateway role trust relation with API integration's API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID by following these instructions, [click here](https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws-common-api-integration-proxy-link.html).

5. Create the external function
```
USE DATABASE USAGE_MONITOR;
CREATE OR REPLACE EXTERNAL FUNCTION usage_monitor_slack(n integer, v varchar)
    RETURNS variant
    IMMUTABLE
    api_integration = usage_monitor_slack_integration
    AS '<resource_invocation_url from cloudformation output>';
```


## Create usage_monitor proc 


## Schedule usage_monitor to run daily



## References

### [1. USAGE view in Snowflake Database](https://docs.snowflake.com/en/sql-reference/account-usage.html)

###	 [2. How to create a Snowflake external function using AWS Lambda](https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws-template.html)

###	 [2. How to create a Snowflake API Intergration](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration.html)
