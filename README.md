# Snowflake Usage Monitoring
## User Story
As a Developer I want a daily usage update that shows how my compute and storage usage is trending to the prior month.

The goal is for this to be similiar to the data we get from AWS Cost Explorer.  

As the Snowflake Views do not seem to have cost info we will show the data in capacity units that it is quoted in,
The METERING_HISTORY for compute does not seem to have a forecast so we will create a simple one based on MTD average where 
	FORECAST = MTD + RemainingDays in month * AVG(MTD Daily Usage)

We will support 2 options in configutaration for output to slack

### Format 1: Just one line for an Account

Account | Forecast |  Trend
12345   | 9,999.9  |  +/-XX%

### Format 2: One row for each Warehouse
<accountId>    | Forecast |  Trend
WAREHOUSE1     | 9,999.9  |  +/-XX%
WAREHOUSE2     | 9,999.9  |  +/-XX%
WAREHOUSE3     | 9,999.9  |  +/-XX%


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

### Your ouput should look like this:
![Image of Cost Explorer](https://github.com/jimzucker/snowflake-usage-monitor/blob/main/images/METRIC_HISTORY_SUMMARY_VIEW.png)

## AWS Slack External Function Installation Instructions



# References

## USAGE view in SNOWFLAKE Datbase
https://docs.snowflake.com/en/sql-reference/account-usage.html

## how to create an external function
https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws-template.html
