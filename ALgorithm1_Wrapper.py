# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 13:04:38 2018
AUTHOR:   Priyanka Vakeel/Koushik -v1
VERSION: v1 (For Cosmos DB)

file_name:Algorithm_Wrapper.py
Objective:execute_r_scripttions as a wrapper program to execute Generate Anomaly Detection algorithm(AlgoID's:5,6,7).
Parameters INPUT : algorithm_name, Flag, StartDateTime, EndDateTime
                Example: Flag = 'true' or 'false'
                         StartDateTime = str('2018-09-17 10:46:36.000') or Null  
                         EndDateTime =  str('2018-09-22 21:50:17.000')  or Null
                         algorithm_name = 'SimpleThresholdValuehCheck' 
           OUPUT: Insert/Update Alarms, AnomalySensorDetails, Messages,Run Details & Status.
                  Example:UPDATE AD.AlgorithmRunStatus SET ExecutionEndTime = '2018/11/23 09:37:47',
                  LastProcessRecord = '2018-09-20 23:59:07' ,RecordsProcess = 5303 ,AnomalyDetectedCount=6 ,
                  Status= 'SUCCESS' WHERE RunID = 545  AND AlgorithmID = 7.
                  Script Successfully Executed
        
"""
# Importing essential libraries.
import datetime
import pandas as pd
import re
import requests

# Importing necessary other Py scripts.
from conf import *
from connectToDb import *
from validationScript_algo1 import *
from GenInd_AD2wSev import *

# Connection to database.
conn = alarms_db_connection()
cursor = conn.cursor()
# Connecting to CosmosDB.
coll_link, client = sensor_db_connect_cosmos()
my_logger = logging.getLogger(__name__)

# def get_sensor_data():


def get_sensor_data(configuration_object, historical_start_date, historical_end_date):

    try:
        options = {}
        options['enableCrossPartitionQuery'] = True
        sensor_query = "select * from " + str(configuration_object['SENSOR_TABLE']) + " s where s.SensorID IN " + str(configuration_object['SENSOR_ID']) + " and s.VesselID = " + str(
            configuration_object['CUSTOMER_VESSEL_ID']) + " and s.TimeStamp >= '" + str(historical_start_date) + "' and s.TimeStamp <='" + str(historical_end_date) + "' order by s.TimeStamp"
#        print(sensor_query)
        sensor_data = client.QueryDocuments(coll_link, sensor_query, options)
        sensor_data = [*sensor_data]
        record_count = len(sensor_data)
        r_input_data1 = pd.DataFrame(sensor_data)
        r_input_data_time_stamp = pd.DataFrame(
            data=r_input_data1, columns=['TimeStamp'])
        r_input_data_time_stamp = r_input_data_time_stamp.drop_duplicates(
            subset='TimeStamp', keep='first', inplace=False)
        r_input_data_time_stamp = r_input_data_time_stamp.astype(
            {'TimeStamp': 'datetime64[s]'})
        r_input_data_time_stamp = r_input_data_time_stamp.rename(
            columns={"TimeStamp": "DateTime"})
        r_input_data_pivot = pd.DataFrame(data=r_input_data1, columns=[
            'TimeStamp', 'SensorID', 'SensorValue'])
        r_input_data_pivot['SensorValue'] = [
            float(data) for data in r_input_data_pivot['SensorValue'].tolist()]
#        print(max(r_input_data_pivot['SensorValue'].tolist()))
        r_input_data_pivot = r_input_data_pivot.astype(
            {'TimeStamp': 'datetime64[s]'})
        r_input_data_pivot = r_input_data_pivot.rename(
            columns={"TimeStamp": "DateTime"})
        r_input_data_pivot = r_input_data_pivot.pivot_table(
            index='DateTime', columns='SensorID', values='SensorValue')
        r_input_data = pd.merge(r_input_data_time_stamp, r_input_data_pivot,
                                left_on='DateTime', right_index=True, how='inner')
        r_input_data = r_input_data.reset_index(drop=True)
        last_processed_record = sensor_data[-1]['TimeStamp']
        file_name = r_input_data1.loc[0, 'FileName']
        file_recieve_date = r_input_data1.loc[0, 'FileReceivedDate']
        file_recieve_date = str(file_recieve_date).replace("-", ',').replace("T", ',').replace(
            ":", ',').replace(".", ',').replace("+", ',').replace(" ", ',').split(",")
        file_recieve_date = (datetime.datetime(int(file_recieve_date[0]), int(file_recieve_date[1]), int(
            file_recieve_date[2]), int(file_recieve_date[3]), int(file_recieve_date[4]), int(file_recieve_date[5])))

        return r_input_data, record_count, last_processed_record, file_name, file_recieve_date  # ,Chunk_Size
    except Exception as e:
        my_logger.exception("Could not fetch sensor data")
#        print('connection to SensorTable failed!', e)


# Identifying Alert Type.
def determine_anomalytype(configuration_object, r_output):
    try:
        anomaly_type1 = []
        for i, r in r_output.iterrows():
            for i in r:
                if i == ' NULL' or i == ' ' or i == 9999:
                    anomaly_type1.append(r['DateTime'])
# Validation of Consecutive occurances of R_Output.
        # Considering columns of r_output except DateTime column.
        check_consecutive = pd.DataFrame(r_output.iloc[:, 1:])
        # Checking for duplicates to identify cosecutiveness of check_consecutive df.
        check_consecutive["is_duplicate"] = check_consecutive.duplicated()
        # considering only duplicated rows of check_consecutive df.
        check_consecutive1 = check_consecutive.loc[check_consecutive['is_duplicate'] == True]
        # collecting indices of row as list.
        check_consecutive1_index = check_consecutive1.index.tolist()
        # Removing 'is_duplicate' columns from r_output.
        check_consecutive1 = check_consecutive1.drop(
            labels='is_duplicate', axis=1)
        # creating a new df for 'DateTime column' with only consecutive row indices in r_output df.
        anomaly_type2_df = pd.DataFrame(
            r_output.iloc[(check_consecutive1_index), 0])
        anomaly_type2 = []
        for index, row in anomaly_type2_df.iterrows():
            anomaly_type2.append(str(row['DateTime']))
        return anomaly_type1, anomaly_type2
#
    except Exception as e:
        my_logger.exception("Could not determine anomaly type")


def get_r_script_results(configuration_object, end_time, start_time):
    try:
        hot_tsq_lim = float(configuration_object['HOT_TSQ_LIM'])
        window_previous_size = int(configuration_object['WINDOW_PREV'])
        window_future_size = int(configuration_object['WINDOW_FUT'])
#        low_severity_thresh = float(configuration_object['BASE_ALARM_VAL'])
#        mid_severity_thresh = float(configuration_object['MID_ALARM_VAL'])
        r_input_data, record_count, last_processed_record, file_name, file_recieve_date = get_sensor_data(
            configuration_object, start_time, end_time)
        # Executing Hotelling T-Sq algorithm Python Script.
        input_schema=r_input_data.columns
        r_output1 = CallCodeIndSc(r_input_data, window_previous_size,
                                         window_future_size, hot_tsq_lim)#, low_severity_thresh, mid_severity_thresh)
        r_output=pd.DataFrame(columns=input_schema)
        r_output=r_input_data.loc[r_input_data['DateTime'].isin(datetime for datetime in r_output1.loc[:,'DateTime'])]
        r_output['AlarmSeverity']=1
        r_output['TestScore']=150.54
#        print(r_output)
        my_logger.info('No of Anomalies generated from Algo: ', len(r_output))
        return file_name, file_recieve_date, last_processed_record, r_output, record_count
    except Exception as e:
        my_logger.exception("Error fetching details from algorithm")


def prepare_alarm_data(configuration_object, start_time, end_time, runid):
    try:
        file_name, file_recieve_date, last_processed_record, r_output, record_count = get_r_script_results(
            configuration_object, end_time, start_time)
        medium_severity = 0
        high_severity = 0
        no_r_output = False
        result = []
        anomaly_count = 0
        datetimestamp = []
        if r_output.empty:
            no_r_output = True
            my_logger.warn('No data available to process as Anomaly generated from R is 0')
            return record_count, len(
                r_output), anomaly_count, datetimestamp, r_output, last_processed_record,medium_severity, high_severity,  no_r_output  # 

        for index, row in r_output.iterrows():
            datetimestamp.append(str(row['DateTime'].replace(tzinfo=None)))
        if len(datetimestamp) == 1:
            datetimestamp = "('" + datetimestamp[0] + "')"
        else:
            datetimestamp = tuple(datetimestamp)
        anomaly_type1, anomaly_type2 = determine_anomalytype(
            configuration_object, r_output)

        # iterate over result dataframe from algorithm and if any results do bulk inserts
        for i, r in r_output.iterrows():
            anomaly_count += 1
            date_time = str(r['DateTime'].replace(tzinfo=None))
            if r['AlarmSeverity'] == 0:
                level = 3  # Green Level
            elif r['AlarmSeverity'] == 1:
                level = 2  # Yellow Level
                medium_severity += 1
            elif r['AlarmSeverity'] == 2:
                level = 1  # Red Level
            high_severity += 1
            if date_time in anomaly_type1:
                anomaly_alert_type_id = 1
            elif date_time in anomaly_type2:
                anomaly_alert_type_id = 2
            else:
                anomaly_alert_type_id = 3
            each_row = [
                runid,
                date_time,
                str(configuration_object['INTERNAL_VESSEL_ID']),
                'Y',
                1,
                level,
                str(r['TestScore']),
                str(file_recieve_date),
                str(file_name),
                anomaly_alert_type_id
            ]
            result.append(each_row)
        # , error_anomalydetect_function
        return record_count, result, anomaly_count, datetimestamp, r_output, last_processed_record, medium_severity, high_severity, no_r_output

    except Exception as e:
        my_logger.exception("prepare_alarm_data function failed!")
        #        error_anomalydetect_function = []
        #        error_anomalydetect_function.append(e)
        #print('prepare_alarm_data function failed!', e)


# error_anomalydetect_function,
def anomaly_detection(configuration_object, start_time, end_time, runid):
    try:
        record_count, result, anomaly_count, datetimestamp, r_output, last_processed_record, medium_severity, high_severity, no_r_output = prepare_alarm_data(
            configuration_object, start_time, end_time, runid)
        if result and no_r_output == False:
            alarms_insert_query = "INSERT INTO "+str(configuration_object['TARGET_TABLE_SCHEMA'])+"."+str(
                configuration_object['TARGET_TABLE'])+" ("+str(alarm_columns)+") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.executemany(alarms_insert_query, result)
#            print('End of Anomaly, ', anomaly_count)
            cursor.commit()
            vessel_cart_summary_api = f"{JOBFUNCTION_URL}/api/JobFunction?code={JOBFUNCTION_CODE}&name=InsertUpdateVesselCartSummary"
            equipment_health_score_api = f"{JOBFUNCTION_URL}/api/JobFunction?code={JOBFUNCTION_CODE}&name=EquipmentHealthScore"
            try:
                response = requests.get(vessel_cart_summary_api)
                response.raise_for_status()
                my_logger.info(vessel_cart_summary_api)
            except Exception as e:
                my_logger.exception("Job Function InsertUpdateVesselCartSummary failed")

            try:
                response = requests.get(equipment_health_score_api)
                response.raise_for_status()
                my_logger.info(equipment_health_score_api)
            except Exception as e:
                my_logger.exception("Job Function EquipmentHealthScore failed")

            alarms_select_query = "SELECT [AlarmID],[AnomalyGeneratedDate] from "+str(configuration_object['TARGET_TABLE_SCHEMA'])+"."+str(
                configuration_object['TARGET_TABLE'])+" where [RunID] = "+str(runid)+" AND [AnomalyGeneratedDate] IN "+str(datetimestamp)+" "
            alarms_data = cursor.execute(alarms_select_query).fetchall()
            alarm_id = pd.DataFrame(alarms_data)

            sensor_id = configuration_object['SENSOR_ID']
            try:
                sensor_detail = []
                sensor_detail = get_insert_rows(
                    alarm_id, configuration_object, r_output, sensor_id)
            except Exception as e:
                pass
                #print('get_insert_rows Function failed!', e)

            sensor_detail_query = "INSERT INTO "+str(SCHEMA)+"."+str(
                anamoly_sensor_details_table)+" ("+str(sensor_detail_column)+") values (?, ?, ?) "
            cursor.executemany(sensor_detail_query, sensor_detail)
            cursor.commit()
#            print(anomaly_count, last_processed_record,
#                  record_count, medium_severity, high_severity)
            cursor.commit()
        error_anomalydetect_function = ''
        return record_count, anomaly_count, last_processed_record, medium_severity, high_severity, error_anomalydetect_function
    except Exception as e:
        error_anomalydetect_function = []
        error_anomalydetect_function.append(e)
        my_logger.exception("Anomaly Detection failed!")
        return 0, 0, last_processed_record, 0, 0, error_anomalydetect_function


def get_insert_rows(alarm_id, configuration_object, r_output, sensor_id):
    sensor_detail_1 = []
    for one_id in sensor_id:
        sensor_detail_id = configuration_object[one_id]
        for index, row in r_output.iterrows():
            each_row1 = []
            for ix, rw in alarm_id.iterrows():
                for i in rw:
                    r_out_datetime = str(row['DateTime'])
                    r_out_datetime1 = str(r_out_datetime).replace(
                        "-", ',').replace(":", ',').replace(" ", ',').split(",")
                    r_output_anomaly_generated_date = (datetime.datetime(int(r_out_datetime1[0]), int(r_out_datetime1[1]), int(
                        r_out_datetime1[2]), int(r_out_datetime1[3]), int(r_out_datetime1[4]), int(r_out_datetime1[5])))
#                            if str(i[1]) == str(row['DateTime'].replace(tzinfo=None)):
                    if str(i[1]) == str(r_output_anomaly_generated_date):
                        # print('#####',r[str(one_id)])
                        each_row1.append(i[0])  # ALARM ID
                        each_row1.append(sensor_detail_id)
                        each_row1.append(row[one_id])  # VALUE
                        sensor_detail_1.append(each_row1)
    return sensor_detail_1


def algorithm_processing(algorithm_name, historical_flag, historical_start_date, historical_end_date):
    try:
        # GET CONFIGURATION: VALIDATES data AVAILABILITY
        start_time1 = datetime.datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        configuration_object, last_processed_record_object = get_configuration(
            algorithm_name, historical_flag, historical_start_date, historical_end_date)

        if last_processed_record_object == 0:
#            print('NO data AVAILABLE FOR PROCESSING')
            end_time1 = datetime.datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
            run_status_query = "INSERT INTO "+str(SCHEMA)+"."+str(run_status_table)+" (AlgorithmID, ExecutionStartTime, ExecutionEndTime, Status, Message) values ( "+str(configuration_object['ALGORITHM_ID'])+",'"+str(
                start_time1)+"','"+str(end_time1)+"', 'Already Processed', 'No new data to be processed, last processed record is: "+str(configuration_object['LAST_PROCESS_TIMESTAMP'])+"') "
            my_logger.info(run_status_query)
            cursor.execute(run_status_query)
            cursor.commit()
        else:
            # Step-1: INSERT INTO Run Table
            start_time1 = datetime.datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
            run_status_query = "INSERT INTO "+str(SCHEMA)+"."+str(run_status_table)+" (AlgorithmID,ExecutionStartTime) values ( "+str(
                configuration_object['ALGORITHM_ID'])+",'"+str(start_time1)+"') "
            cursor.execute(run_status_query)
            my_logger.info(run_status_query)
            cursor.commit()
            id_query = "SELECT CAST(@@Identity AS INTEGER)"
            id_inserted = cursor.execute(id_query).fetchone()
            runid = re.sub('[(){}<>,] ', '', str(id_inserted))
            runid = re.findall('\d+', str(id_inserted))
            total_anomaly = 0
            total_record_count = 0
            total_medium_count = 0
            total_high_count = 0
            run_detail_failed_chunk = []

            for i in last_processed_record_object:
                get_counts_params = dict(
                    runid=runid,
                    total_anomaly=total_anomaly,
                    total_high_count=total_high_count,
                    total_medium_count=total_medium_count,
                    total_record_count=total_record_count
                )
                last_processed_record, run_detail_failed_chunk1, total_anomaly, total_high_count, total_medium_count, \
                    total_record_count = get_counts_from_algorithm(
                        configuration_object, i, run_detail_failed_chunk, get_counts_params)
#            print(total_anomaly, total_record_count, last_processed_record,
#                  total_medium_count, total_high_count)
#            print('##################')
            historical_params = dict(
                historical_end_date=historical_end_date,
                historical_flag=historical_flag,
                historical_start_date=historical_start_date
            )
            run_detail_params = dict(
                run_detail_failed_chunk=run_detail_failed_chunk,
                run_detail_failed_chunk1=run_detail_failed_chunk1,
                runid=runid, total_anomaly=total_anomaly,
                total_high_count=total_high_count, total_medium_count=total_medium_count,
                total_record_count=total_record_count
            )
            update_message_and_run_status(configuration_object, historical_params,
                                          last_processed_record, run_detail_params)
    except Exception as e:
        my_logger.exception('algorithm_processing Function failed!')


def update_message_and_run_status(configuration_object, historical_params,
                                  last_processed_record, run_detail_params):
    try:
        historical_end_date = historical_params['historical_end_date']
        historical_flag = historical_params['historical_flag']
        historical_start_date = historical_params['historical_start_date']

        run_detail_failed_chunk = run_detail_params['run_detail_failed_chunk']
        run_detail_failed_chunk1 = run_detail_params['run_detail_failed_chunk1']
        runid = run_detail_params['runid']
        total_anomaly = run_detail_params['total_anomaly']
        total_high_count = run_detail_params['total_high_count']
        total_medium_count = run_detail_params['total_medium_count']
        total_record_count = run_detail_params['total_record_count']
        # Step-5: Message table Insertion:::
        end_time1 = datetime.datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        message = configuration_object['EQUIPMENT_NAME'] + " on Vessel " + \
            configuration_object['CUSTOMER_VESSEL_NAME'] + " has an Alert"
        if total_medium_count >= 1:
            #            print(total_medium_count)
            message_query = "INSERT INTO " + str(SCHEMA) + "." + str(
                messages_table) + " ([MessageText], [GeneratedDate], [RunID], [IsDeleted], [CustomerID], [SeverityId]) values ( '" + str(
                message) + "', '" + str(end_time1) + "' , " + str(runid[0]) + ", 'N', "+str(configuration_object['CUSTOMER_ID'])+", 2) "  # "+str(CUSTOMER_ID)+"
            cursor.execute(message_query)
            cursor.commit()
        if total_high_count >= 1:  # "+str(CUSTOMER_ID)+"[abs-sqldb-offshore]
            #            print(total_high_count)
            message_query = "INSERT INTO " + str(SCHEMA) + "." + str(
                messages_table) + " ([MessageText], [GeneratedDate], [RunID], [IsDeleted], [CustomerID], [SeverityId]) values ( '" + str(
                message) + "', '" + str(end_time1) + "' , " + str(runid[0]) + ", 'N', "+str(configuration_object['CUSTOMER_ID'])+", 1) "
            cursor.execute(message_query)
            cursor.commit()
        # Step-6: END Run table execution
        if historical_flag == 'true' and len(run_detail_failed_chunk) != 0:
            update_run_status_query = "UPDATE " + str(SCHEMA) + "." + str(
                run_status_table) + " SET ExecutionEndTime = '" + str(end_time1) + "',HistoricalFrom='" + str(
                historical_start_date) + "', HistoricalTo='" + str(historical_end_date) + "', LastProcessRecord = '" + str(
                last_processed_record) + "' ,RecordsProcess = " + str(
                total_record_count) + " ,AnomalyDetectedCount=" + str(
                total_anomaly) + " ,Status= 'SUCCESS',Message= 'Failedchunk details " + str(
                run_detail_failed_chunk1) + "'  WHERE RunID = " + str(runid[0]) + "  AND AlgorithmID = " + str(
                configuration_object['ALGORITHM_ID']) + " "  # chunk details "+str(run_detail_failed_chunk)+"'
        elif historical_flag == 'true' and len(run_detail_failed_chunk) == 0:
            update_run_status_query = "UPDATE " + str(SCHEMA) + "." + str(
                run_status_table) + " SET ExecutionEndTime = '" + str(end_time1) + "',HistoricalFrom='" + str(
                historical_start_date) + "', HistoricalTo='" + str(historical_end_date) + "', LastProcessRecord = '" + str(
                last_processed_record) + "' ,RecordsProcess = " + str(total_record_count) + " ,AnomalyDetectedCount=" + str(
                total_anomaly) + " ,Status= 'SUCCESS',Message= 'Success' WHERE RunID = " + str(
                runid[0]) + "  AND AlgorithmID = " + str(configuration_object['ALGORITHM_ID']) + " "
        if historical_flag == 'false' and len(run_detail_failed_chunk) != 0:
            update_run_status_query = "UPDATE " + str(SCHEMA) + "." + str(
                run_status_table) + " SET ExecutionEndTime = '" + str(end_time1) + "',HistoricalFrom='" + str(
                historical_start_date) + "', HistoricalTo='" + str(historical_end_date) + "', LastProcessRecord = '" + str(
                last_processed_record) + "' ,RecordsProcess = " + str(
                total_record_count) + " ,AnomalyDetectedCount=" + str(
                total_anomaly) + " ,Status= 'SUCCESS',Message= 'Failed chunk details " + str(
                run_detail_failed_chunk1) + "' WHERE RunID = " + str(runid[0]) + "  AND AlgorithmID = " + str(
                configuration_object['ALGORITHM_ID']) + " "
        elif historical_flag == 'false' and len(run_detail_failed_chunk) == 0:
            update_run_status_query = "UPDATE " + str(SCHEMA) + "." + str(
                run_status_table) + " SET ExecutionEndTime = '" + str(end_time1) + "',HistoricalFrom='" + str(
                historical_start_date) + "', HistoricalTo='" + str(historical_end_date) + "', LastProcessRecord = '" + str(
                last_processed_record) + "' ,RecordsProcess = " + str(total_record_count) + " ,AnomalyDetectedCount=" + str(
                total_anomaly) + " ,Status= 'SUCCESS',Message= 'Success' WHERE RunID = " + str(
                runid[0]) + "  AND AlgorithmID = " + str(configuration_object['ALGORITHM_ID']) + " "
        cursor.execute(update_run_status_query)
#        print(update_run_status_query)
        cursor.commit()
        cursor.close()
    except Exception as e:
        my_logger.exception('update_message_and_run_status Function failed!', e)


def get_counts_from_algorithm(configuration_object, record_object, run_detail_failed_chunk, params):
    try:
        runid = params['runid']
        total_anomaly = params['total_anomaly']
        total_high_count = params['total_high_count']
        total_medium_count = params['total_medium_count']
        total_record_count = params['total_record_count']
        start_time = record_object[0]
        end_time = record_object[1]
        current_processed_record_object = str(record_object)
        current_processed_record_object = current_processed_record_object.replace(
            "'", '"')
        # Step-2: Execution for RunDetail Table
        start_time2 = datetime.datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        run_detail_status_query = "INSERT INTO " + str(SCHEMA) + "." + str(
            run_detail_status_table) + " (RunID, VesselID, AlgorithmAppliedOn, AlgorithmAppliedOnValue, ExecutionStartTime) values ( " + str(
            runid[0]) + ", " + str(configuration_object['INTERNAL_VESSEL_ID']) + " , 'VESSELID'," + str(
            configuration_object['INTERNAL_VESSEL_ID']) + " , '" + str(start_time2) + "') "
        cursor.execute(run_detail_status_query)
        cursor.commit()
        detail_query = "SELECT CAST(@@Identity AS INTEGER)"
        detail_inserted = cursor.execute(detail_query).fetchone()
        rundetailid = re.sub('[(){}<>,] ', '', str(detail_inserted))
        rundetailid = re.findall('\d+', str(detail_inserted))
        # Step-3: Execute detect Anomaly: returns: record_count, anomaly_count,last_processed_record,medium_severity,high_severity
#        import pdb;pdb.set_trace()
        record_count, anomaly_count, last_processed_record, medium_severity, high_severity, error_anomalydetect_function = anomaly_detection(
            configuration_object, start_time, end_time, runid[0])
        #                print(record_count, anomaly_count,last_processed_record,medium_severity,high_severity,error_anomalydetect_function)
#        print('*************************')
        # Step-4: Update End Rundetail Table
        end_time2 = datetime.datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        run_detail_failed_chunk1 = None
        if error_anomalydetect_function == '':
            update_run_status_detail_query = "UPDATE " + str(SCHEMA) + "." + str(
                run_detail_status_table) + " SET ExecutionEndTime = '" + str(end_time2) + "',LastProcessRecord = '" + str(
                last_processed_record) + "' ,RecordsProcess = " + str(
                record_count) + " ,AnomalyDetectedCount=" + str(
                anomaly_count) + " ,Status= 'SUCCESS',Message= 'Success' WHERE RunDetailId = " + str(
                rundetailid[0]) + " AND RunID = " + str(runid[0]) + " "
        elif len(error_anomalydetect_function) != 0:
            update_run_status_detail_query = "UPDATE " + str(SCHEMA) + "." + str(
                run_detail_status_table) + " SET ExecutionEndTime = '" + str(end_time2) + "',LastProcessRecord = '" + str(
                end_time) + "' ,RecordsProcess = " + str(
                record_count) + " ,AnomalyDetectedCount=" + str(
                anomaly_count) + " ,Status= 'FAILED', Message= 'Failed chunk details " + str(
                current_processed_record_object) + "' WHERE RunDetailId = " + str(rundetailid[0]) + " AND RunID = " + str(
                runid[0]) + " "
            run_detail_failed_chunk.append((current_processed_record_object))
            if len(current_processed_record_object) != 0:
                run_detail_failed_chunk1 = (
                    str(run_detail_failed_chunk).replace("'", ''))
        cursor.execute(update_run_status_detail_query)
        cursor.commit()
        total_anomaly += anomaly_count
        total_record_count += record_count
        total_medium_count += medium_severity
        total_high_count += high_severity
        return last_processed_record, run_detail_failed_chunk1, total_anomaly, total_high_count, total_medium_count, total_record_count
    except Exception as e:
        my_logger.exception('get_counts_from_algorithm Function failed!', e)


#if __name__ == '__main__':
#    algorithm_name = 'Algo1_11_Cosmos'
#    historical_flag = 'true'
#    historical_start_date = '2019-04-08 14:10:52.000'
#    historical_end_date = '2019-04-08 15:10:52.000'
#    algorithm_execution_start_time = datetime.datetime.now().replace(microsecond=0)
#    print('Started: ', algorithm_execution_start_time)
#    algorithm_processing(algorithm_name, historical_flag,
#                         historical_start_date, historical_end_date)
#    print('Script Successfully Executed')
#    algorithm_execution_end_time = datetime.datetime.now().replace(microsecond=0)
#    algorithm_total_execution_time = algorithm_execution_end_time - \
#        algorithm_execution_start_time
#    print('ENDED: ', algorithm_total_execution_time)
