
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

/*
    Update API_AWS_ROLE_ARN & API_ALLOWED_PREFIXES to output of running your cloud formation stack
*/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION  usage_monitor
    API_PROVIDER = aws_api_gateway
--    API_PROVIDER = aws_private_api_gateway 
    API_AWS_ROLE_ARN = 'arn:aws:iam::<id>:role/snowflake-usage-monitor-agw-role'
    API_ALLOWED_PREFIXES = ('https://<id>.execute-api.us-east-1.amazonaws.com/snowflake-usage-monitor-stage/slack_post')
    ENABLED = TRUE
    COMMENT = 'Post Usage Monitoring data to Slack'
    ;

-- Update the API Gateway role trust relation with API integration's API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION usage_monitor;