# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 13:04:38 2018
File:Validationscript.py
Objective: Script fuctions as validating the configuration details and Dateranges.
Parameters INPUT: algorithm_name,historical_flag,historical_start_date,historical_end_date
               Example: historical_flag = 'true' or 'false'
                         historical_start_date = str('2018-09-17 10:46:36.000') or Null  
                         historical_end_date =  str('2018-09-22 21:50:17.000')  or Null
                         algorithm_name = 'SimpleThresholdValuehCheck' 
           OUPUT: dict(configuration_object), last_processed_record_object
               Example:
        
        
AUTHOR:   Priyanka Vakeel/Sai Koushik   -Version2
VERSION: Version2 (For Cosmos DB)
"""

# Validation and Configuration Script

# Step-1: DB Engine conf from Conf.py for Configuration
# Step-2: Get conf details required for this Algorithm
# Step-3: Check the availabilty of Data in DWH table

# Importing essential libraries.
import math

# Importing necessary other Py scripts.
from conf import *
from connectToDb import *

my_logger = logging.getLogger(__name__)

# Connecting to DataBase.
conn = alarms_db_connection()

if not conn:
    my_logger.error("Could not connect to Database")

cursor = conn.cursor()

# Connecting to CosmosDB.
coll_link, client = sensor_db_connect_cosmos()


def query_cosmos_with_options(chunk_size, sql_query):
    """
    Query Cosmos DB with optimized options
    :param chunk_size: number of records to be fetched
    :param sql_query: SQL query
    :return: list of record sets
    """
    options = {
        'enableCrossPartitionQuery': True,
        'maxItemCount': chunk_size,
        'continuation': True,
    }
    result = client.QueryDocuments(coll_link, sql_query, options)
    record_set = list(result)
    return record_set


def get_chunk_date_range(sensor_table, vessel_id, sensor_id, last_processed_record, chunk_size, historical_flag_settings):
    """

    Parameters Input: sensor_table, vessel_id, sensor_id, last_processed_record, chunk_size, historical_flag, historical_start_date, historical_end_date.
                    Example: SensorData,111,1210,'2018-09-20 19:31:06',1000,true/false,'2018-09-17 10:47:45', '2018-09-20 19:31:06',511.

               Output: last_processed_record_object.
                    Example:    
    """
    historical_flag = historical_flag_settings['historical_flag']
    historical_start_date = historical_flag_settings['historical_start_date']
    historical_end_date = historical_flag_settings['historical_end_date']
    try:
        chunk_size = int(chunk_size)
        options = {}
        options['enableCrossPartitionQuery'] = True
        options['maxItemCount'] = chunk_size
        options['continuation'] = True

        if historical_flag == 'true':
            historical_last_processed_sensor_query = "select value count(1) from " + str(sensor_table) + " s where s.SensorID IN " + str(sensor_id) + " and s.VesselID = " + str(
                vessel_id) + " and s.TimeStamp >= " + "'" + str(historical_start_date) + "'" + " and s.TimeStamp <=" + "'" + str(historical_end_date) + "'"

            total_record_size = query_cosmos_with_options(
                chunk_size, historical_last_processed_sensor_query)
            my_logger.info(historical_last_processed_sensor_query)
#            print(historical_last_processed_sensor_query)
            total_record_size = [(i,) for i in total_record_size]
            total_record_count = total_record_size[0][0]
            historical_last_processed_sensor_data_query = "select s.TimeStamp from " + str(sensor_table) + " s where s.SensorID IN " + str(sensor_id) + " and s.VesselID = " + str(
                vessel_id) + " and s.TimeStamp >= " + "'" + str(historical_start_date) + "'" + " and s.TimeStamp <=" + "'" + str(historical_end_date) + "' order by s.TimeStamp"
            my_logger.info(historical_last_processed_sensor_query)
            total_sensor_data = query_cosmos_with_options(
                chunk_size, historical_last_processed_sensor_data_query)
            total_sensor_data = pd.DataFrame(total_sensor_data)
            total_sensor_data = list(total_sensor_data['TimeStamp']) if not total_sensor_data.empty else []
        elif historical_flag == 'false':
            total_record_size, total_sensor_data = process_non_historic_chunk(sensor_table,chunk_size, last_processed_record,
                                                                              sensor_id, vessel_id)
            total_record_count = total_record_size[0]

        if total_record_count == 0:
            my_logger.warn("No Sensor Data Found")
            last_processed_record_object = 0
            return last_processed_record_object
        my_logger.info(f"{total_record_count} Sensor Data Records Found" )
        last_processed_start_time, last_processed_end_time = [], []
        number_of_chunk = list(
            range(int(math.floor(total_record_size[0][0] / chunk_size))))
        
        if len(total_sensor_data) <= chunk_size:
            if not historical_flag:
                last_processed_start_time.append(
                    str(historical_start_date))
                last_processed_end_time.append(str(historical_end_date))
                last_processed_record_object = list(
                    zip(last_processed_start_time, last_processed_end_time))
            last_processed_start_time.append(str(total_sensor_data[0]))
            last_processed_end_time.append(str(total_sensor_data[-1]))
            last_processed_record_object = list(
                zip(last_processed_start_time, last_processed_end_time))
        for i in number_of_chunk:
            get_last_processed_start_time(chunk_size, i, last_processed_end_time, last_processed_start_time,
                                          number_of_chunk, total_sensor_data)
        start_time, end_time = [], []
        for i in list(range(len(last_processed_start_time))):
            start_time.append(last_processed_start_time[i])
        for i in list(range(len(last_processed_start_time))):
            end_time.append(last_processed_end_time[i])
        value2 = [str(i) for i in start_time]
        value3 = [str(i) for i in end_time]
        last_processed_record_object = list(zip(value2, value3))
        return last_processed_record_object
    except Exception as e:
        my_logger.exception('Unable to proceed the LastProcessedRec!,get_chunk_date_range Function Failed ')


def get_last_processed_start_time(chunk_size, i, last_processed_end_time, last_processed_start_time, number_of_chunk,
                                  total_sensor_data):
    if i == 0:
        last_processed_start_time.append(total_sensor_data[i])
        last_processed_end_time.append(
            total_sensor_data[(chunk_size - 1)])
    else:
        if i >= 1:
            last_processed_start_time.append(
                total_sensor_data[(i * chunk_size)])
            last_processed_end_time.append(
                total_sensor_data[(((i * chunk_size) + chunk_size) - 1)])
        if i == max(number_of_chunk):
            remained_data = list(
                total_sensor_data[(((i * chunk_size) + chunk_size) + 1):])
            if remained_data != []:
                last_processed_start_time.append(
                    remained_data[0])
                last_processed_end_time.append(
                    remained_data[-1])

def process_non_historic_chunk(sensor_table,chunk_size, last_processed_record, sensor_id, vessel_id):
    if last_processed_record == 'None':
        complete_last_processed_sensor_query = "select value count(1) from " + str(sensor_table) + " s where s.SensorID IN " + str(
            sensor_id) + " and s.VesselID = " + "" + str(vessel_id) + ""
#        print(complete_last_processed_sensor_query)
        total_record_size = query_cosmos_with_options(
            chunk_size, complete_last_processed_sensor_query)
        total_record_size = [(i,) for i in total_record_size]

        complete_last_processed_sensor_data_query = "select s.TimeStamp from " + str(sensor_table) + " s where s.SensorID IN " + \
                                                    str(sensor_id) + " and s.VesselID = " + "" + \
                                                    str(vessel_id) + "" + \
            " order by s.TimeStamp"
        #
        #                    complete_last_processed_sensor_data_query="select s.TimeStamp from sensordata s where s.SensorID = " + str(1029) + " OR s.SensorID =" + str(779)+" OR s.SensorID =" + str(964)+" OR s.SensorID =" + str(445)+" OR s.SensorID =" + str(466)+ " and s.VesselID = " + ""+ str(vessel_id) + "" + " order by s.TimeStamp"
        total_sensor_data = query_cosmos_with_options(
            chunk_size, complete_last_processed_sensor_data_query)
        total_sensor_data = pd.DataFrame(total_sensor_data)
        total_sensor_data = list(total_sensor_data['TimeStamp'])

    else:
        complete_last_processed_sensor_query = "select value count(1) from " + str(sensor_table) + " s where s.SensorID IN " + str(
            sensor_id) + " and s.VesselID = " + str(vessel_id) + " and s.TimeStamp > '" + str(
            last_processed_record) + "'"
#        print(complete_last_processed_sensor_query)
        total_record_size = query_cosmos_with_options(
            chunk_size, complete_last_processed_sensor_query)
        if total_record_size[0] != 0:
            total_record_size = [(i,) for i in total_record_size]
            complete_sensor_data_query = "select s.TimeStamp from " + str(sensor_table) + " s where s.SensorID IN " + \
                                         str(sensor_id) + " and s.VesselID = " + str(vessel_id) + \
                                         " and s.TimeStamp > '" + \
                                         str(last_processed_record) + \
                                         "' order by s.TimeStamp"
            total_sensor_data = query_cosmos_with_options(
                chunk_size, complete_sensor_data_query)
            total_sensor_data = pd.DataFrame(total_sensor_data)
            total_sensor_data = list(
                total_sensor_data['TimeStamp'])
        else:
            total_sensor_data = []
    return total_record_size, total_sensor_data


def call_sp_get_algorithm_configdata(algorithm_name):
    """
    Call the 'spGetAlgorithmConfigdata' stored proceedure to fetch the Algorithm basic details like Algo Configuration details,Sensor ID,Last Process Timestamp etc.
    """
    try:
        final_configuration_data = []
        parameter_algorithm_name = str(algorithm_name)
        algorithm_configuration_data = cursor.execute(
            "{CALL AD.spGetAlgorithmConfigdata (?)}", parameter_algorithm_name).fetchall()
        configuration_data = [(i[1], i[2])
                              for i in algorithm_configuration_data]
        final_configuration_data.extend(configuration_data)
        keys_column1 = ['INTERNAL_VESSEL_ID', 'CUSTOMER_VESSEL_ID', 'ALGORITHM_ID',
                        'CUSTOMER_VESSEL_NAME', 'COMPONENT_NAME', 'EQUIPMENT_NAME']
        value1 = [int(algorithm_configuration_data[0][4]), int(algorithm_configuration_data[0][5]), int(algorithm_configuration_data[0][0]), str(
            algorithm_configuration_data[0][8]), str(algorithm_configuration_data[0][9]), str(algorithm_configuration_data[0][10])]
        configuration1 = dict(zip(keys_column1, value1))

        sensor_detail = []
        sensor_id = []

        if cursor.nextset() == True:
            for rec in cursor:
                sensor_id.append(rec[0])
                sensor_detail.append(rec)
        last_record = []
        if cursor.nextset():
            last_record = ['None' if last == ' ' else last for last in cursor]

# Handling LAST_PROCESS_TIMESTAMP dynamically.
        if last_record == []:
            last_record_value = 'None'
        else:
            last_record_value = last_record[0][0]
        # To Retrive Customer_ID Dynamically.
        customer_id = 0
        if cursor.nextset() == True:
            for cust_id in cursor:
                customer_id = cust_id[0]

        return final_configuration_data, configuration1, sensor_detail, sensor_id, last_record_value, customer_id
    except Exception as e:
        my_logger.exception('call_sp_get_algorithm_configdata Function failed! ')


def get_configuration(algorithm_name, historical_flag, historical_start_date, historical_end_date):
    """
    Purpose: Fetching configuration details along with date ranges based on chunk size.
    Parameters Input: algorithm_name,historical_flag,historical_start_date,historical_end_date.
                    Example: confObj,'2018-09-17 10:47:45', '2018-09-20 19:31:06'.
               Output: DATA_chunk_size, SENSOR_TABLE, SENSORGROUPID, TARGET_TABLE_SCHEMA, TARGET_TABLE, THRESHOLD_VALUE, BASE_ALARM_VAL,
                       MID_ALARM_VAL, INTERNAL_VESSEL_ID, CUSTOMER_VESSEL_ID, ALGORITHM_ID, CUSTOMER_VESSEL_NAME, COMPONENT_NAME,
                       EQUIPMENT_NAME, sensor_id, SENSORVALS, LAST_PROCESS_TIMESTAMP.

                    Example: {'DATA_chunk_size': '1000', 'SENSOR_TABLE': 'SensorData', 'SENSORGROUPID': '5', 'TARGET_TABLE_SCHEMA': 'AD',
                    'TARGET_TABLE': 'Alarms_old', 'THRESHOLD_VALUE': '2.610679', 'BASE_ALARM_VAL': '2.755716', 'MID_ALARM_VAL': '3.0',
                    1210: '36', 'INTERNAL_VESSEL_ID': 2, 'CUSTOMER_VESSEL_ID': 111, 
                    'ALGORITHM_ID': 9, 'CUSTOMER_VESSEL_NAME': 'Hellas Whistler', 
                    'COMPONENT_NAME': 'line shaft bearing 3 mpde 3', 'EQUIPMENT_NAME': 'Bearings', 
                    'sensor_id': 1210, 'SENSORVALS': '[1210]', 'LAST_PROCESS_TIMESTAMP': 'None'} 
                    [('2018-09-17 10:47:45', '2018-09-20 19:31:06'), ('2018-09-20 19:33:07', '2018-09-22 21:50:17')]
    """
    try:
        final_configuration_data, configuration1, sensor_detail, sensor_id, last_record_value, customer_id = call_sp_get_algorithm_configdata(
            algorithm_name)

        # To retrieve the data for the Anamoly Issue Type
        if cursor.nextset():
            [rec2 for rec2 in cursor]

        sensor_column = []
        final_configuration_data.extend(sensor_detail)
        sensor_column = ['['+str(i)+']' for i in sensor_id]
        sens_column = ','.join(sensor_column)
        key_column2 = ['SENSOR_ID', 'SENSORVALS',
                       'LAST_PROCESS_TIMESTAMP', 'CUSTOMER_ID']
        value2 = [tuple(sensor_id), sens_column,
                  last_record_value, customer_id]
        configuration2 = dict(zip(key_column2, value2))
        final_configuration_data = [list(i) for i in final_configuration_data]
        configuration_object = {row[0]: row[1]
                                for row in final_configuration_data}
        configuration_object.update(configuration1)
        configuration_object.update(configuration2)
        hist_settings = {
            True: dict(
                historical_flag=historical_flag,
                historical_start_date=historical_start_date,
                historical_end_date=historical_end_date
            ),
            False: dict(
                historical_flag='false',
                historical_start_date=0,
                historical_end_date=0
            )
        }
        is_hist_flag_set = historical_flag == 'true' and (
            historical_start_date != ' ' or historical_end_date != ' ')
        historical_flag_settings = hist_settings[is_hist_flag_set]
        last_processed_record_object = get_chunk_date_range(configuration_object['SENSOR_TABLE'], configuration_object['CUSTOMER_VESSEL_ID'], configuration_object[
            'SENSOR_ID'], configuration_object['LAST_PROCESS_TIMESTAMP'], configuration_object['DATA_CHUNK_SIZE'], historical_flag_settings)
#        print(dict(configuration_object), last_processed_record_object)
        return dict(configuration_object), last_processed_record_object
    except Exception as e:
        my_logger.exception('Connection to Configuration failed!,get_configuration Function Failed')

    
#if __name__ == '__main__':
#        algorithm_name = 'Algo1_11_Cosmos'
#        historical_flag = 'true'
#        historical_start_date = '2019-02-16 23:05:56.000'
#        historical_end_date = '2019-02-17 21:58:27.000'
#        get_configuration(algorithm_name, historical_flag,
#                          historical_start_date, historical_end_date)

