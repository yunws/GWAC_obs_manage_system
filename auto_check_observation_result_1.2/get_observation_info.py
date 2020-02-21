import psycopg2
import sys
import json
import time
import datetime
import psycopg2.extras
import os

def Yunwei_DBConnect(location):
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        yunwei_host = data["yunwei_host_bj"]
    elif location == 'xinglong':
        yunwei_host = data["yunwei_host_xl"]
    db=psycopg2.connect("host=" + yunwei_host + " port= " + data["yunwei_port"] + \
        " dbname='" + data["yunwei_db"] + "' user='" + data["yunwei_user"] + "' password='"  + data["yunwei_password"] + "'")
    return db

#__________________________________
def Yunwei_DBClose(db):
    "Close the connection to the DB"
    db.close()

# def check_observartion_gwac(check_time, location='xinglong'):
#     db = Yunwei_DBConnect(location='xinglong')
#     if db != []:
#         try:
#             cursor = db.cursor()
#             query = ("SELECT object_list_all.obj_id, \
#             object_list_all.objsour, \
#             object_list_all.obj_name, \
#             object_running_list_current.begin_time, \
#             object_running_list_current.end_time \
#             FROM object_list_all, \
#             object_running_list_current, \
#             object_list_current \
#             WHERE object_list_all.obj_id = object_list_current.obj_id \
#             and object_list_all.obj_id = object_running_list_current.obj_id \
#             and object_list_current.date_cur = \'" + check_time + "\' \vim 
#             and object_list_current.obs_stag in ('complete', 'break', 'pass') \
#             and (object_list_all.objsour LIKE '%GW_initial%' \
#             or  object_list_all.objsour LIKE '%GW_update%' \
#             or object_list_all.objsour LIKE '%Fermi_GRB_%')")
#             print(query)
#             cursor.execute(query)
#             result = cursor.fetchall()
#             print(result)
#             cursor.close()
#             Yunwei_DBClose(db)
#         except:
#             result = []
#             print("lost connection to PostqreSQL server during the query_(check_observation_gwac)")
#     return result


def check_trigger(check_time, location='xinglong'):
    db = Yunwei_DBConnect(location='xinglong')
    if db != []:
        try:
            cursor = db.cursor()
            query = ("SELECT object_list_all.obj_id, \
            object_running_list_current.op_sn, \
            object_running_list_current.group_id, \
            object_running_list_current.unit_id, \
            object_running_list_current.grid_id, \
            object_running_list_current.field_id, \
            object_running_list_current.ra, \
            object_running_list_current.dec, \
            object_list_all.objsour, \
            object_list_all.obj_name, \
            object_running_list_current.obstype, \
            object_running_list_current.priority, \
            object_list_current.obs_stag \
            FROM object_list_all, \
            object_running_list_current, \
            object_list_current \
            WHERE object_list_all.obj_id = object_list_current.obj_id \
            and object_list_all.obj_id = object_running_list_current.obj_id \
            and object_list_current.date_cur = \'" + check_time + "\' \
            and object_list_current.obs_stag in ('complete', 'break', 'pass') \
            and (object_list_all.objsour LIKE '%GW_initial%' \
            or  object_list_all.objsour LIKE '%GW_update%' \
            or object_list_all.objsour LIKE '%Fermi_GRB_%')")
            print(query)
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            Yunwei_DBClose(db)
            # print(result)
        except:
            result = []
            print("lost connection to PostqreSQL server during the query_check_trigger")
        gwac_observation_log = []
        xl60cm_observation_log = []
        xl30cm_observation_log = []
        for row in result:
            if row[2] == 'XL001':
                gwac_observation_log.append(row)
            elif row[2] == 'XL002':
                xl60cm_observation_log.append(row)
            elif row[2] == 'XL003':
                xl30cm_observation_log.append(row)
        # print(gwac_observation_log)
        # print(xl60cm_observation_log)
        # print(xl30cm_observation_log)

            
    return gwac_observation_log, xl60cm_observation_log, xl30cm_observation_log



def check_routine_observation( check_time,  location='xinglong'):
    db = Yunwei_DBConnect(location='xinglong')
    if db != []:
        try:
            cursor = db.cursor()
            query = ("SELECT object_list_all.obj_id, \
            object_running_list_current.op_sn, \
            object_running_list_current.group_id, \
            object_running_list_current.unit_id, \
            object_running_list_current.grid_id, \
            object_running_list_current.field_id, \
            object_running_list_current.ra, \
            object_running_list_current.dec, \
            object_list_all.objsour, \
            object_list_all.obj_name, \
            object_running_list_current.obstype, \
            object_running_list_current.priority, \
            object_list_current.obs_stag \
            FROM object_list_all, \
            object_running_list_current, \
            object_list_current \
            WHERE object_list_all.obj_id = object_list_current.obj_id \
            and object_list_all.obj_id = object_running_list_current.obj_id \
            and object_list_current.date_cur = \'" + check_time + "\' \
            and object_list_current.obs_stag in ('complete', 'break', 'pass') \
            and object_list_all.priority < '60' ")
            print(query)
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            Yunwei_DBClose(db)
            # print(result)
        except:
            result = []
            print("lost connection to PostqreSQL server during the query_(check_routine)")
        gwac_observation_log = []
        xl60cm_observation_log = []
        xl30cm_observation_log = []
        for row in result:
            if row[2] == 'XL001':
                gwac_observation_log.append(row)
            elif row[2] == 'XL002':
                xl60cm_observation_log.append(row)
            elif row[2] == 'XL003':
                xl30cm_observation_log.append(row)
        # print(gwac_observation_log)
        # print(xl60cm_observation_log)
        # print(xl30cm_observation_log)

            
    return gwac_observation_log, xl60cm_observation_log, xl30cm_observation_log



search_time = '2020/02/04'
#check_observartion_gwac(search_time)
print(check_routine_observation(search_time))
# print(check_trigger(search_time)) 