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
                TO_CHAR((SUM(FORECAST) - SUM(PRIOR_MONTH))/SUM(PRIOR_MONTH)*100, '99,999.0"%"') "CHANGE"
        FROM (
          SELECT CURRENT_ACCOUNT() as ACCOUNT, CREDITS_USED, 0 PRIOR_MONTH, 0 "CHANGE", FORECAST FROM METERING_HISTORY_TEMPTB
          WHERE START_TIME_MONTH = (SELECT MAX(START_TIME_MONTH) FROM METERING_HISTORY_TEMPTB)
          UNION ALL
          SELECT CURRENT_ACCOUNT(), 0, CREDITS_USED, 0, 0 FROM METERING_HISTORY_TEMPTB
          WHERE START_TIME_MONTH = (SELECT MIN(START_TIME_MONTH) FROM METERING_HISTORY_TEMPTB)
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
            ORDER BY 1;`
       } );
       
var who_cares = snowflake.execute( { sqlText:
      `CREATE OR REPLACE TEMPORARY TABLE METERING_HISTORY_NAME_TREND(ACCOUNT VARCHAR(25), "MTD" VARCHAR(25), "FORECAST" VARCHAR(25), "PRIOR_MONTH" VARCHAR(25), "CHANGE" VARCHAR(25) );`
       } );

var who_cares = snowflake.execute( { sqlText:
      `INSERT INTO METERING_HISTORY_NAME_TREND
       SELECT NAME, TO_CHAR(SUM(CREDITS_USED),'999,999.00') "MTD", 
                TO_CHAR(SUM(FORECAST),'999,999.00') "FORECAST", 
                TO_CHAR(SUM(PRIOR_MONTH),'999,999.00') "PRIOR_MONTH", 
                TO_CHAR((SUM(FORECAST) - SUM(PRIOR_MONTH))/SUM(PRIOR_MONTH)*100, '99,999.0"%"') "CHANGE"
        FROM (
          SELECT NAME, CREDITS_USED, 0 PRIOR_MONTH, 0 "CHANGE", FORECAST FROM METERING_HISTORY_NAME_TEMPTB
          WHERE START_TIME_MONTH = (SELECT MAX(START_TIME_MONTH) FROM METERING_HISTORY_NAME_TEMPTB)
          UNION ALL
          SELECT NAME, 0, CREDITS_USED, 0, 0 FROM METERING_HISTORY_NAME_TEMPTB
          WHERE START_TIME_MONTH = (SELECT MIN(START_TIME_MONTH) FROM METERING_HISTORY_NAME_TEMPTB)
        )
        GROUP BY 1;`
       } );

  var who_cares = snowflake.execute( { sqlText:
       `DROP TABLE METERING_HISTORY_NAME_TEMPTB`
       } );
       
  return 'Done.';
$$;

