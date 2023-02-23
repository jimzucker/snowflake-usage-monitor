/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

CREATE DATABASE IF NOT EXISTS USAGE_MONITOR;

USE DATABASE USAGE_MONITOR;

CREATE OR REPLACE PROCEDURE CALC_METERING_HISTORY_TREND() 
RETURNS VARCHAR
LANGUAGE javascript
as
$$
var who_cares = snowflake.execute( { sqlText:
        `CREATE OR REPLACE TEMPORARY TABLE METERING_HISTORY_TEMPTB(START_TIME_MONTH int, CREDITS_USED double, FORECAST double );`
       } );
       
var who_cares = snowflake.execute( { sqlText:
        `INSERT INTO METERING_HISTORY_TEMPTB
            SELECT Top 2 START_TIME_MONTH, CREDITS_USED, FORECAST FROM
              (
                SELECT MONTH(START_TIME) + YEAR(START_TIME)*100 START_TIME_MONTH,
                    SUM(CREDITS_USED_COMPUTE) CREDITS_USED_COMPUTE,
                    SUM(CREDITS_USED_CLOUD_SERVICES) CREDITS_USED_CLOUD_SERVICES,
                    SUM(CREDITS_USED) CREDITS_USED,
                    SUM(CREDITS_USED) + ( AVG(CREDITS_USED) * ( last_day(to_date(getdate())) - to_date(MAX(START_TIME)) ) )  FORECAST
                FROM "SNOWFLAKE"."ACCOUNT_USAGE"."METERING_HISTORY"
                WHERE MONTH(START_TIME) = MONTH(getdate())
                GROUP BY 1
                UNION ALL
                SELECT MONTH(START_TIME) + YEAR(START_TIME)*100 START_TIME_MONTH,
                    SUM(CREDITS_USED_COMPUTE) CREDITS_USED_COMPUTE,
                    SUM(CREDITS_USED_CLOUD_SERVICES) CREDITS_USED_CLOUD_SERVICES,
                    SUM(CREDITS_USED) CREDITS_USED,
                    SUM(CREDITS_USED) FORECAST
                FROM "SNOWFLAKE"."ACCOUNT_USAGE"."METERING_HISTORY"
                WHERE MONTH(START_TIME) < MONTH(getdate())
                GROUP BY 1
                UNION ALL
                    SELECT 10990101, 0.01, 0.01, 0.01, 0.1
                )
            ORDER BY 1;`
       } );


/* ---------------------------------------------------------------- */
/* -- Create metrics for each Warehouse                          -- */
/* ---------------------------------------------------------------- */
var who_cares = snowflake.execute( { sqlText:
      `CREATE OR REPLACE TEMPORARY TABLE METERING_HISTORY_TREND(ACCOUNT VARCHAR(25), "MTD" VARCHAR(25), "FORECAST" VARCHAR(25), "PRIOR_MONTH" VARCHAR(25), "CHANGE" VARCHAR(25) );`
       } );
       
var who_cares = snowflake.execute( { sqlText:
      `INSERT INTO METERING_HISTORY_TREND
       SELECT ACCOUNT, TO_CHAR(SUM(CREDITS_USED),'999,999.00') "MTD", 
                TO_CHAR(SUM(FORECAST),'999,999.00') "FORECAST", 
                TO_CHAR(SUM(PRIOR_MONTH),'999,999.00') "PRIOR_MONTH", 
                TO_CHAR((SUM(FORECAST) - SUM(PRIOR_MONTH))/SUM(PRIOR_MONTH)*100, '999,999.0"%"') "CHANGE"
        FROM (
          SELECT CURRENT_ACCOUNT() as ACCOUNT, CREDITS_USED, 0 PRIOR_MONTH, 0 "CHANGE", FORECAST FROM METERING_HISTORY_TEMPTB
          WHERE START_TIME_MONTH = (SELECT MAX(START_TIME_MONTH) FROM METERING_HISTORY_TEMPTB)
          UNION ALL
          SELECT CURRENT_ACCOUNT(), 0, CREDITS_USED, 0, 0 FROM METERING_HISTORY_TEMPTB
          WHERE START_TIME_MONTH = (SELECT MIN(START_TIME_MONTH) FROM METERING_HISTORY_TEMPTB)
        )
        GROUP BY 1;`
       } );

/* -- Add Storage tracking in GB -- */
var who_cares = snowflake.execute( { sqlText:
      `INSERT INTO METERING_HISTORY_TREND
          SELECT 'DATABASE_BYTES(GB)' ACCOUNT, TO_CHAR(SUM(MTD/1073741824),'999,999,999.000') "MTD", 
                TO_CHAR(SUM(FORECAST/1073741824),'999,999,999.000') "FORECAST", 
                TO_CHAR(SUM(PRIOR_MONTH/1073741824),'999,999,999.000') "PRIOR_MONTH", 
                TO_CHAR((SUM(FORECAST/1073741824) - SUM(PRIOR_MONTH/1073741824))/SUM(PRIOR_MONTH/1073741824)*100, '999,999.0"%"') "CHANGE"
          FROM (
            SELECT CURRENT_ACCOUNT() ACCOUNT,SUM(AVERAGE_DATABASE_BYTES) MTD, 0 PRIOR_MONTH
              , SUM(AVERAGE_DATABASE_BYTES) + ( AVG(AVERAGE_DATABASE_BYTES) * ( last_day(to_date(getdate())) - to_date(MAX(USAGE_DATE)) ) )  FORECAST
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY"
            WHERE  DATE_PART('YEAR',USAGE_DATE) = DATE_PART('YEAR', getdate()) and DATE_PART('MONTH',USAGE_DATE) = DATE_PART('MONTH', getdate())
            UNION ALL 
            SELECT CURRENT_ACCOUNT() ACCOUNT, 0 MTD, SUM(AVERAGE_DATABASE_BYTES) PRIOR_MONTH, SUM(AVERAGE_DATABASE_BYTES) FORECAST
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY"
            WHERE  DATE_PART('YEAR',USAGE_DATE) = DATE_PART('YEAR', dateadd(day, -30, getdate())) and DATE_PART('MONTH',USAGE_DATE) = DATE_PART('MONTH', dateadd(day, -30, getdate()))
            UNION ALL
            SELECT CURRENT_ACCOUNT() ACCOUNT, 0 MTD, 1 PRIOR_MONTH, 1 FORECAST
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY"
            WHERE  NOT EXISTS ( 
              SELECT * FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY" 
              WHERE DATE_PART('YEAR',USAGE_DATE) = DATE_PART('YEAR', dateadd(day, -30, getdate())) and DATE_PART('MONTH',USAGE_DATE) = DATE_PART('MONTH', dateadd(day, -30, getdate()))
              )
          )
          GROUP BY 1;`
       } );
       
  var who_cares = snowflake.execute( { sqlText:
       `DROP TABLE METERING_HISTORY_TEMPTB`
       } );

/* ---------------------------------------------------------------- */
/* -- Create metrics for each Warehouse                          -- */
/* ---------------------------------------------------------------- */

var who_cares = snowflake.execute( { sqlText:
        `CREATE OR REPLACE TEMPORARY TABLE METERING_HISTORY_NAME_TEMPTB(NAME VARCHAR(25), START_TIME_MONTH int, CREDITS_USED double, FORECAST double );`
       } );
var who_cares = snowflake.execute( { sqlText:
        `INSERT INTO METERING_HISTORY_NAME_TEMPTB
            SELECT NAME, START_TIME_MONTH, CREDITS_USED, FORECAST FROM
              (
                SELECT NAME, MONTH(START_TIME) + YEAR(START_TIME)*100 START_TIME_MONTH,
                    SUM(CREDITS_USED_COMPUTE) CREDITS_USED_COMPUTE,
                    SUM(CREDITS_USED_CLOUD_SERVICES) CREDITS_USED_CLOUD_SERVICES,
                    SUM(CREDITS_USED) CREDITS_USED,
                    SUM(CREDITS_USED) + ( AVG(CREDITS_USED) * ( last_day(to_date(getdate())) - to_date(MAX(START_TIME)) ) )  FORECAST
                FROM "SNOWFLAKE"."ACCOUNT_USAGE"."METERING_HISTORY"
                WHERE MONTH(START_TIME) = MONTH(getdate())
                GROUP BY 1,2
                UNION ALL
                SELECT NAME, MONTH(START_TIME) + YEAR(START_TIME)*100 START_TIME_MONTH,
                    SUM(CREDITS_USED_COMPUTE) CREDITS_USED_COMPUTE,
                    SUM(CREDITS_USED_CLOUD_SERVICES) CREDITS_USED_CLOUD_SERVICES,
                    SUM(CREDITS_USED) CREDITS_USED,
                    SUM(CREDITS_USED) FORECAST
                FROM "SNOWFLAKE"."ACCOUNT_USAGE"."METERING_HISTORY"
                WHERE MONTH(START_TIME) < MONTH(getdate())
                GROUP BY 1,2
                HAVING MONTH(START_TIME) + YEAR(START_TIME)*100 = MAX(MONTH(START_TIME) + YEAR(START_TIME)*100)
                )
            ORDER BY FORECAST DESC;`
       } );
       
var who_cares = snowflake.execute( { sqlText:
      `CREATE OR REPLACE TEMPORARY TABLE METERING_HISTORY_NAME_TREND(NAME VARCHAR(25), "MTD" VARCHAR(25), "FORECAST" VARCHAR(25), "PRIOR_MONTH" VARCHAR(25), "CHANGE" VARCHAR(25) );`
       } );

var who_cares = snowflake.execute( { sqlText:
      `INSERT INTO metering_history_name_trend
SELECT 'Snowflake Usage' NAME
	,To_char(mtd.usage_in_currency, '999,999.00') "MTD"
	,To_char((
			(
				SELECT Sum(usage_in_currency)
				FROM snowflake.organization_usage.usage_in_currency_daily
				WHERE usage_date > Dateadd(day, - 30, CURRENT_TIMESTAMP())
				) / 30
			) * (Extract(day FROM Last_day(CURRENT_TIMESTAMP())) - Extract(day FROM CURRENT_TIMESTAMP())) + mtd.usage_in_currency, '999,999.00') "FORECAST"
	,To_char((
			SELECT Sum(usage_in_currency)
			FROM snowflake.organization_usage.usage_in_currency_daily
			WHERE usage_date > Dateadd(month, - 2, CURRENT_TIMESTAMP())
				AND usage_date < Dateadd(month, - 1, CURRENT_TIMESTAMP())
			), '999,999.00') "PRIOR_MONTH"
	,To_char((mtd.usage_in_currency - (
				SELECT Sum(usage_in_currency)
				FROM snowflake.organization_usage.usage_in_currency_daily
				WHERE usage_date > Dateadd(month, - 2, CURRENT_TIMESTAMP())
					AND usage_date < Dateadd(month, - 1, CURRENT_TIMESTAMP())
				)) / (
				SELECT Sum(usage_in_currency)
				FROM snowflake.organization_usage.usage_in_currency_daily
				WHERE usage_date > Dateadd(month, - 2, CURRENT_TIMESTAMP())
					AND usage_date < Dateadd(month, - 1, CURRENT_TIMESTAMP())
				) * 100, '999,999.0"%"') "CHANGE"
FROM (
	SELECT Round(Sum(usage_in_currency), 2) AS usage_in_currency
	FROM snowflake.organization_usage.usage_in_currency_daily
	WHERE usage_date >= Trunc(CURRENT_TIMESTAMP(), 'MONTH')
		AND usage_date <= CURRENT_TIMESTAMP()
	) mtd
UNION ALL
SELECT NAME,
       To_char(Sum(credits_used)*2, '999,999.00') "MTD",
       To_char(Sum(forecast)*2, '999,999.00')     "FORECAST",
       To_char(Sum(prior_month)*2, '999,999.00')  "PRIOR_MONTH",
       To_char(( Sum(forecast)*2 - Sum(prior_month)*2 ) / Sum(prior_month)*2 * 100,
       '999,999.0"%"')                             "CHANGE"
FROM   (SELECT NAME,
               credits_used,
               0 PRIOR_MONTH,
               0 "CHANGE",
               forecast
        FROM   metering_history_name_temptb
        WHERE  start_time_month = (SELECT Max(start_time_month)
                                   FROM   metering_history_name_temptb)
        UNION ALL
        SELECT NAME,
               0,
               credits_used,
               0,
               0
        FROM   metering_history_name_temptb
        WHERE  start_time_month = (SELECT Min(start_time_month)
                                   FROM   metering_history_name_temptb))
GROUP  BY 1
HAVING Sum(prior_month) != 0
UNION ALL
SELECT NAME,
       To_char(Sum(credits_used)*2, '999,999.00') "MTD",
       To_char(Sum(forecast)*2, '999,999.00')     "FORECAST",
       To_char(Sum(prior_month)*2, '999,999.00')  "PRIOR_MONTH",
       To_char(Sum(forecast)*2, '999,999.0"%"')    "CHANGE"
FROM   (SELECT NAME,
               credits_used,
               0 PRIOR_MONTH,
               0 "CHANGE",
               forecast
        FROM   metering_history_name_temptb
        WHERE  start_time_month = (SELECT Max(start_time_month)
                                   FROM   metering_history_name_temptb)
        UNION ALL
        SELECT NAME,
               0,
               credits_used,
               0,
               0
        FROM   metering_history_name_temptb
        WHERE  start_time_month = (SELECT Min(start_time_month)
                                   FROM   metering_history_name_temptb))
GROUP  BY 1
HAVING Sum(prior_month) = 0
ORDER  BY forecast DESC;`
       } );

/* -- Add Storage tracking in GB -- */
    var who_cares = snowflake.execute( { sqlText:
      `INSERT INTO METERING_HISTORY_NAME_TREND
          SELECT 'DATABASE_BYTES(GB)' ACCOUNT, TO_CHAR(SUM(MTD/1073741824),'999,999,999.000') "MTD", 
                TO_CHAR(SUM(FORECAST/1073741824),'999,999,999.000') "FORECAST", 
                TO_CHAR(SUM(PRIOR_MONTH/1073741824),'999,999,999.000') "PRIOR_MONTH", 
                TO_CHAR((SUM(FORECAST/1073741824) - SUM(PRIOR_MONTH/1073741824))/SUM(PRIOR_MONTH/1073741824)*100, '999,999.0"%"') "CHANGE"
          FROM (
            SELECT CURRENT_ACCOUNT() ACCOUNT,SUM(AVERAGE_DATABASE_BYTES) MTD, 0 PRIOR_MONTH
              , SUM(AVERAGE_DATABASE_BYTES) + ( AVG(AVERAGE_DATABASE_BYTES) * ( last_day(to_date(getdate())) - to_date(MAX(USAGE_DATE)) ) )  FORECAST
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY"
            WHERE  DATE_PART('YEAR',USAGE_DATE) = DATE_PART('YEAR', getdate()) and DATE_PART('MONTH',USAGE_DATE) = DATE_PART('MONTH', getdate())
            UNION ALL 
            SELECT CURRENT_ACCOUNT() ACCOUNT, 0 MTD, SUM(AVERAGE_DATABASE_BYTES) PRIOR_MONTH, SUM(AVERAGE_DATABASE_BYTES) FORECAST
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY"
            WHERE  DATE_PART('YEAR',USAGE_DATE) = DATE_PART('YEAR', dateadd(day, -30, getdate())) and DATE_PART('MONTH',USAGE_DATE) = DATE_PART('MONTH', dateadd(day, -30, getdate()))
            UNION ALL
            SELECT CURRENT_ACCOUNT() ACCOUNT, 0 MTD, 1 PRIOR_MONTH, 1 FORECAST
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY"
            WHERE  NOT EXISTS ( 
              SELECT * FROM "SNOWFLAKE"."ACCOUNT_USAGE"."DATABASE_STORAGE_USAGE_HISTORY" 
              WHERE DATE_PART('YEAR',USAGE_DATE) = DATE_PART('YEAR', dateadd(day, -30, getdate())) and DATE_PART('MONTH',USAGE_DATE) = DATE_PART('MONTH', dateadd(day, -30, getdate()))
              )
          )
          GROUP BY 1;`
       } );
       
  var who_cares = snowflake.execute( { sqlText:
       `DROP TABLE METERING_HISTORY_NAME_TEMPTB`
       } );
       
  return 'Done.';
$$;