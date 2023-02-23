# Snowflake Usage Monitoring
## User Story
As a Developer I want a daily usage update that shows how my compute and storage usage is trending to the prior month.

The goal is for this to be similiar to the data we get from AWS Cost Explorer.  

As the Snowflake View for per Warehouse usage does not track Usage in currency, we need to get the usage in credits, and then multiply it by the Cost Per Credit. 
This can be either $2 per credit on a Basic account, or $3.7 per credit on an Enterprise Account. The SQL is currently configured to run on a Basic account.
If you have an Enterprise Account, please refer to METERING_HISTORY_TREND.sql to see how to configure the app to accurately reflect your costs.

The METERING_HISTORY for compute does not seem to have a forecast either, so we will create a simple one based on MTD average where 
	FORECAST = MTD + RemainingDays in month * AVG(MTD Daily Usage)

We will support 2 options in configutaration for output to slack

### Format 1: Just one line for an Account

Account             | Forecast*    | Change
-------------------------------------------
12345               | $9,999.9     | +/-XX%
DATABASE_BYTES(GB)  | 9,999.9     | +/-XX%

### Format 2: One row for each Warehouse

Account             | Forecast*          | Change
-------------------------------------------
Snowflake Usage     | $9,999.9           | +/-XX%
WAREHOUSE1          | $4,444.4           | +/-XX%
WAREHOUSE2          | $3,333.3           | +/-XX%
CLOUD_SERVICES_ONLY | $2,222.2           | +/-XX%
DATABASE_BYTES(GB)  |  1,111.1           | +/-XX%


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





## AWS Lambda External Function Installation Instructions
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

CREATE OR REPLACE API INTEGRATION  usage_monitor_lambda_integration
    API_PROVIDER = aws_api_gateway
--    API_PROVIDER = aws_private_api_gateway 
    API_AWS_ROLE_ARN = 'arn:aws:iam::<id>:role/snowflake-usage-monitor-agw-role'
    API_ALLOWED_PREFIXES = ('https://<id>.execute-api.us-east-1.amazonaws.com/snowflake-usage-monitor-stage/')
    ENABLED = TRUE
    COMMENT = 'Post Usage Monitoring data to Slack'
    ;

-- Update the API Gateway role trust relation with API integration's API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION usage_monitor_lambda_integration;
```

4. Update the API Gateway role trust relation with API integration's API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID by following these instructions, [click here](https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws-common-api-integration-proxy-link.html).


5. Create the external function
```
USE DATABASE USAGE_MONITOR;
CREATE OR REPLACE EXTERNAL FUNCTION usage_monitor_lambda(v varchar, l integer, o varchar)
    RETURNS variant
    api_integration = usage_monitor_lambda_integration
    AS '<resource_invocation_url from cloudformation output>';
```


6. Create usage_monitor proc 

# Run one of these depending on if you want to run on METERING_HISTORY_NAME_TREND or METERING_HISTORY_TREND
```
CREATE OR REPLACE PROCEDURE run_usage_monitor_lambda()
RETURNS INT
LANGUAGE SQL
AS $$
BEGIN
    CALL CALC_METERING_HISTORY_TREND();
    SELECT usage_monitor_lambda(name, forecast, change) 
    FROM metering_history_name_trend
    ORDER BY forecast DESC;
END;
$$;
```
or 
```
CREATE OR REPLACE PROCEDURE run_usage_monitor_lambda()
RETURNS INT
LANGUAGE SQL
AS $$
BEGIN
    CALL CALC_METERING_HISTORY_TREND();
    SELECT usage_monitor_lambda(account, forecast, change) 
    FROM metering_history_trend
    ORDER BY forecast DESC;
END;
$$;
```

7. Schedule usage_monitor to run daily by creating a task
```
CREATE OR REPLACE TASK daily_monitor
COMMENT = 'Task to run usage monitor lambda'
AS
CALL run_usage_monitor_lambda();

```

8. and then schedule it. 
# You can make your own CRON if you want,but here,  #1 runs it at midnight UTC, and #2 runs it at noon UTC.
```
ALTER TASK daily_monitor SET SCHEDULE = 'USING CRON 0 0 * * * UTC';
```
or
```
ALTER TASK daily_monitor SET SCHEDULE = 'USING CRON 12 0 * * * UTC';
```




## References

### [1. USAGE view in Snowflake Database](https://docs.snowflake.com/en/sql-reference/account-usage.html)

###	 [2. How to create a Snowflake external function using AWS Lambda](https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws-template.html)

###	 [2. How to create a Snowflake API Intergration](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration.html)



## Development Journal
7-Nov Setup External function infrastructure on AWS lambda and updated documentation (5h)<br>
7-Nov Researched adding Storage and updated views (3h)<br>
6-Nov Created Primay view or usage monitoring (5 hour)<br>