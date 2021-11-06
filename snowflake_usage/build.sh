#!/bin/sh
set -e
DEPLOYMENT_PACKAGE=snowflake-usage-pkg.zip
mkdir -p target
if [ ! -f ./target/snowflake-usage-pkg.zip ]
then
	pip install --target ./package snowflake-connector-python==2.4.5
	(cd package && zip -r ../target/$DEPLOYMENT_PACKAGE .)
fi
(cd target && zip -g $DEPLOYMENT_PACKAGE ../snowflake_usage.py)
#aws s3 cp $DEPLOYMENT_PACKAGE s3://lambdalatestfn
