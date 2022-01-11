# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 15:54:31 2018

@author: pvakeel/Koushik
"""

import pyodbc
import pandas as pd
from conf import *
from pydocumentdb import document_client

my_logger = logging.getLogger(__name__)


def sensor_db_connect_cosmos():
    try:
        client = document_client.DocumentClient(uri, {'masterKey': key})
        db_query = "select * from r where r.id = '{0}'".format(db_id)
        db = list(client.QueryDatabases(db_query))[0]
        db_link = db['_self']
        coll_query = "select * from r where r.id = '{0}'".format(coll_id)
        coll = list(client.QueryCollections(db_link, coll_query))[0]
        coll_link = coll['_self']
        return coll_link, client
    except Exception as e:
        my_logger.warn("Could not Connect to COSMOS DB")
        my_logger.critical(f"'cosmos_db_uri': {uri}, 'cosmos_db_key': {key}, 'cosmosdb_id':{db_id}, 'cosmosdb_coll_id':{coll_id}")
        return None, None


def alarms_db_connection():
    try:
        dbconnection = pyodbc.connect('DRIVER='+DRIVER+';SERVER='+ALARM_SERVER_NAME +
                                      ';PORT='+str(PORT)+';DATABASE='+ALARM_DATABASE+';UID='+ALARM_USERNAME+';PWD=' + ALARM_PASSWORD)
        return dbconnection
    except Exception as e:
        my_logger.warn("Could not Connect to MSSQL DB")
        my_logger.critical(f"'sqldb_url':{ALARM_SERVER_NAME}, 'sqldb_name': {ALARM_DATABASE}, 'sqldb_port':{PORT}, 'sqldb_user':{ALARM_USERNAME}, sqldb_passwd: {ALARM_PASSWORD}")
        return None
