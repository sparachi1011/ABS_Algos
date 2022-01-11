# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 11:57:43 2018

@author: pvakeel
"""

import os
import logging

# Uncomment below code-block for local
# For Production, add these as variables in Application settings for the webjob

"""
os.environ['cosmos_db_uri'] = 'https://abs-cosmos-dev.documents.azure.com:443/'
os.environ['cosmosdb_key'] = 'sPXEqkqDmH85fVT4493hDJ6X0qXcwJ7qKWiSzw7JK43655Yv4s8wYfxCwZdYcdTzFNDhyciGGaXIHwwBuor8fw== '
os.environ['cosmosdb_id'] = 'cosmosdb_poc'
os.environ['cosmosdb_coll_id'] = 'sensordata'
os.environ['sqldb_port'] = '1433'
os.environ['sqldb_url'] = 'abs-asql.database.windows.net'
os.environ['sqldb_name'] = 'abs-sqldb'
os.environ['sqldb_user'] = 'absadmin@abs-asql'
os.environ['sqldb_passwd'] = 'P@ssword1234'
os.environ['sqldb_schema'] = 'AD'
os.environ['jobfunction_url'] = 'https://absjobs.azurewebsites.net'
os.environ['jobfunction_code'] = 'kqnFefWEus2VFcHau9wk1Q0NOKN7an/zTWJC2ubUMMobWWyfw/lmgw=='
os.environ['algorithm_name'] = 'Algo1_11_Cosmos'
os.environ['historical_flag'] = 'false'
os.environ['historical_start_date'] = '2019-04-08 14:10:52.000'
os.environ['historical_end_date'] = '2019-04-08 15:10:52.000'
"""

# COSMOS DB Settings
uri = os.getenv('cosmos_db_uri')
key = os.getenv('cosmosdb_key')

# MSSQL DB Settings
DRIVER = '{ODBC Driver 13 for SQL Server}'
PORT = os.getenv('sqldb_port')
db_id = os.getenv('cosmosdb_id')
coll_id = os.getenv('cosmosdb_coll_id')  # collection ID is name of your collection

ALARM_SERVER_NAME = os.getenv('sqldb_url')
ALARM_DATABASE = os.getenv('sqldb_name')

ALARM_USERNAME = os.getenv('sqldb_user')
ALARM_PASSWORD = os.getenv('sqldb_passwd')
SCHEMA = os.getenv('sqldb_schema')

JOBFUNCTION_URL = os.getenv('jobfunction_url')
JOBFUNCTION_CODE = os.getenv('jobfunction_code')

# Algorithm Run Settings
algorithm_name = os.getenv('algorithm_name')
historical_flag = os.getenv('historical_flag')
historical_start_date = os.getenv('historical_start_date')
historical_end_date = os.getenv('historical_end_date')

run_status_table = 'AlgorithmRunStatus'
run_detail_status_table = 'AlgorithmRunDetailStatus'
anamoly_sensor_details_table = 'AnomalySensorDetails'
messages_table = 'Messages'

sensor_detail_column = 'AlarmID, SensorID,SensorValue'
alarm_columns = 'RunId, AnomalyGeneratedDate,VesselId,Anomaly,AnomalyID,AnomalySeverityId,AlgorithmScore,FileReceivedDate,FileName,AnomalyAlertTypeID'

# set up logging to file
LOGS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "run_logs")
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logging.basicConfig(level=logging.INFO,
                    # format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    format="[%(levelname)-8s %(asctime)s %(filename)s:%(lineno)s - %(funcName)20s()] %(message)s",
                    datefmt='%m-%d %H:%M',
                    filename=os.path.join(LOGS_DIR, "anomalyjobrun.log"),
                    filemode='w')
