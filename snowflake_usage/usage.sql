
CREATE or replace PROCEDURE CALC_USAGE_TREND() 
RETURNS VARCHAR
LANGUAGE javascript
as
$$
var who_cares = snowflake.execute( { sqlText:
        `CREATE OR REPLACE TEMPORARY TABLE USAGE_SUMMARY(Month int, CREDITS_USED double );`
       } );
       
var who_cares = snowflake.execute( { sqlText:
        `INSERT INTO USAGE_SUMMARY
            SELECT Top 2 Month, CREDITS_USED FROM
              (
                SELECT MONTH(START_TIME) + YEAR(START_TIME)*100 Month,
                    SUM(CREDITS_USED_COMPUTE) CREDITS_USED_COMPUTE,
                    SUM(CREDITS_USED_CLOUD_SERVICES) CREDITS_USED_CLOUD_SERVICES,
                    SUM(CREDITS_USED) CREDITS_USED
                FROM "SNOWFLAKE"."ACCOUNT_USAGE"."METERING_HISTORY"
                GROUP BY 1
                UNION ALL
                    SELECT 10990101, 0.01, 0.01, 0.01
                ORDER BY 1
                )
            ORDER BY 1;`
       } );

var who_cares = snowflake.execute( { sqlText:
      `CREATE OR REPLACE TEMPORARY TABLE USAGE_TREND(Account VARCHAR(25), "Current" VARCHAR(25), "Prior" VARCHAR(25), "Change" VARCHAR(25) );`
       } );
       
var who_cares = snowflake.execute( { sqlText:
      `INSERT INTO USAGE_TREND
       SELECT Account "Account", TO_CHAR(SUM(CREDITS_USED),'999,999.00') "Current", TO_CHAR(SUM(Prior),'999,999.00') "Prior", TO_CHAR((SUM(CREDITS_USED)  - SUM(Prior))/SUM(Prior)*100, '999.0"%"') "Change"
        FROM (
          SELECT CURRENT_ACCOUNT() as Account, CREDITS_USED, 0 Prior, 0 "Change" FROM USAGE_SUMMARY
          WHERE MONTH = (SELECT MAX(MONTH) FROM USAGE_SUMMARY)
          UNION ALL
          SELECT CURRENT_ACCOUNT() as Account, 0, CREDITS_USED, 0 FROM USAGE_SUMMARY
          WHERE MONTH = (SELECT MIN(MONTH) FROM USAGE_SUMMARY)
        )
        GROUP BY 1;`
       } );

var who_cares = snowflake.execute( { sqlText:
       `DROP TABLE USAGE_SUMMARY`
       } );
  return 'Done.';
$$;

call CALC_USAGE_TREND();
SELECT * from USAGE_TREND;
