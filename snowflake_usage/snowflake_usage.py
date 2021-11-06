import snowflake.connector


PASSWORD = '<PASSWORD>'
USER = '<USER>'
ACCOUNT = '<ACCOUNT>'
WAREHOUSE = '<WAREHOUSE>'
DATABASE = 'USAGE_MONITOR'
SCHEMA = 'PUBLIC'
ROLE = 'PUBLIC'

def lambda_handler(event, context):
	print("Connecting...")
	con = snowflake.connector.connect(
		user=USER,
		url='<ACCOUNTNAME>.snowflakecomputing.com',
		account=ACCOUNT,
		password=PASSWORD,
		warehouse=WAREHOUSE,
		database=DATABASE,
		schema=SCHEMA
	)

	try:
		result = cur.execute("SELECT * from <TABLENAME>")
		print(result)
		result_list = result.fetchall()
		print(result_list)
	except:
		"Database call failed"
	finally:
		cur.close()


con.close()
print('------------------END----------------')

