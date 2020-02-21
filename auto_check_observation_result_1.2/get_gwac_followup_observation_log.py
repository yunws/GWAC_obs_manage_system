
import sys
import os
import datetime
import time
import json
#import mysql.connector
import numpy as np
from func_gwac_too_image_status_query import func_gwac_too_image_status_query
from first_and_last_image import first_and_last_image
# load the adapter
import pymysql
import psycopg2
# load the psycopg extras module
import psycopg2.extras
# from get_observation_info_GW_GRB import check_trigger
from get_observation_info import check_trigger
try:
  sys.path.append("./coor_convert/")
  from dd2dms import dd2dms
  from dd2hms import dd2hms
except:
  print("please install sidereal code ")


#__________________________________
def CMM_DBConnect(location):
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        CMM_user = data["CMM_user_bj"]
    elif location == 'xinglong':
        CMM_user = data["CMM_user_xl"]
    # db = pymysql.connect(data["CMM_host"],CMM_user,data["CMM_password"],data["CMM_db"] )
    try:
        db = pymysql.connect(data["CMM_host"],CMM_user,data["CMM_password"],data["CMM_db"] )
    except:
        print('can not connect to the CMM DB')
        db = []
    return db

#__________________________________
def CMM_DBClose(db):
    "Close the connection to the DB"
    db.close()

#__________________________________
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

def Retrieve_trigger_name(location,obj_sour_trigger):
    "Find the trigger ID and name"
    db = CMM_DBConnect(location)
    # print(db)
    if db != []:
        try:
            cursor = db.cursor()
            query = "SELECT external_trigger_name FROM trigger_v where external_trigger_type_simulation=1 and ID_external_trigger =  \'" + obj_sour_trigger + "\'"
            print(query)
            cursor.execute(query)
            results = cursor.fetchall()
            # print(results)
            if not results:
                # ID_external_trigger = []
                external_trigger_name = []
                # print('xxxxxxxxxx')
            else:
                # ID_external_trigger = []
                external_trigger_name = []
                external_trigger_name.append(results[0][0])
                # print(external_trigger_name)

            cursor.close()
            CMM_DBClose(db)
        except:
            print("3. lost connection to Mysql server during the query")
            # ID_external_trigger = []
            external_trigger_name = []
    else:
        # ID_external_trigger = []
        external_trigger_name = []
    return external_trigger_name


# time_now = (datetime.datetime.now() + datetime.timedelta(hours= -24)).strftime("%Y/%m/%d")
# print(time_now)
# time_now = '2019/12/16'
# gwac_observation_log = check_trigger(time_now)[0]
# print(gwac_observation_log)
# for i in gwac_observation_log:
#     print(i)
#     obj_sour_trigger_all = i[8]
#     print(obj_sour_trigger_all)
#     print(obj_sour_trigger_all)
#     obj_sour_trigger = obj_sour_trigger_all.split('_')[-2]
#     print(obj_sour_trigger)
#     external_trigger_name = Retrieve_trigger_name('xinglong',obj_sour_trigger)
#     print(external_trigger_name)

def Retrieve_trigger_id(location,trigger_name):
    "Find the ID from the trigger name"
    db = CMM_DBConnect(location)
    # print(db)
    if db != []:
        try:
            cursor = db.cursor()
            query = "SELECT * FROM trigger_v where external_trigger_type_simulation=1 and external_trigger_name=\'" + trigger_name + "\'"
            # print(query)
            cursor.execute(query)
            results = cursor.fetchall()
            # print(results)
            if not results:
                ID_external_trigger = []
            else:
                ID_external_trigger = []
                for n in range(len(results)):
                    ID_external_trigger.append(results[n][0])
            cursor.close()
            CMM_DBClose(db)
        except:
            print("2. lost connection to Mysql server during the query")
            ID_external_trigger = []
    else:
        ID_external_trigger = []
    return ID_external_trigger

# if __name__ == '__main__':
# #    print sys.argv  
#     if not sys.argv[1:]:
#         sys.argv += [ "S191129u", "2019-12-01T16:04:36", "32800", "33600"]

#     trigger_name = sys.argv[1]
#     search_time_str = sys.argv[2]
#     check_window_pre = float(sys.argv[3])
#     check_window_post = float(sys.argv[4])

Field_ID = []
finally_result = []
def get_gwac_trigger_info():
    global Field_ID
    time_now = (datetime.datetime.now() + datetime.timedelta(hours= -24)).strftime("%Y/%m/%d")
    time_test = '2020/01/14'
    gwac_observation_log = check_trigger(time_now)[0]
    # print(gwac_observation_log)
    # print(len(gwac_observation_log))
    object_sour_all = []
    for i in gwac_observation_log:
        print(i)
        obj_sour_trigger_all = i[8]
        obj_sour_trigger_pre = obj_sour_trigger_all.split('_')[-2]
        if obj_sour_trigger_pre not in object_sour_all:
            object_sour_all.append(obj_sour_trigger_pre)
    print(object_sour_all)
    object_name_all = []
    for obj_sour_trigger in object_sour_all:
        print(obj_sour_trigger)
        trigger_name_pre  = Retrieve_trigger_name('xinglong',obj_sour_trigger)[0]
        if trigger_name_pre not in object_name_all:
            object_name_all.append(trigger_name_pre)
    timenow = (datetime.datetime.utcnow())
    search_time_str =  datetime.datetime.strftime(timenow,'%Y-%m-%dT%H:%M:%S')
    test_search_time_str = '2020-01-14T12:32:12'
    check_window_pre = "32800"
    check_window_post = "33600"

    location = 'xinglong'
    if location == 'xinglong':
        configuration_file = 'configuration_xl.dat'
    elif location == 'beijing':
        configuration_file = 'configuration_bj.dat'
    # print(trigger_name_pre)
    for trigger_name in object_name_all:
        print(trigger_name)
        ID_external_trigger_arr = Retrieve_trigger_id(location,trigger_name)
        if len(ID_external_trigger_arr) == 0:
            print('no trigger name matched')
        else:
            # print(ID_external_trigger_arr)
            # utc_time_str = test_search_time_str
            utc_time_str = search_time_str
            utc_datetime = datetime.datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%S')
            check_window_pre = float(check_window_pre)/3600.0
            check_window_post = float(check_window_post)/3600.0
            utc_datetime_begin = utc_datetime - datetime.timedelta(hours=check_window_pre)
            utc_datetime_end = utc_datetime + datetime.timedelta(hours=check_window_post)
            utc_datetime_begin_str = datetime.datetime.strftime(utc_datetime_begin, '%Y-%m-%d %H:%M:%S')
            utc_datetime_end_str = datetime.datetime.strftime(utc_datetime_end, '%Y-%m-%d %H:%M:%S')
            utc_datetime_begin_str_T = datetime.datetime.strftime(utc_datetime_begin, '%Y-%m-%dT%H:%M:%S')
            utc_datetime_end_str_T = datetime.datetime.strftime(utc_datetime_end, '%Y-%m-%dT%H:%M:%S')

            currenttime_time  = time.gmtime()
            currenttime_str  = time.strftime("%Y-%m-%d %H:%M:%S", currenttime_time)
            currenttime_datetime = datetime.datetime.strptime(currenttime_str, '%Y-%m-%d %H:%M:%S')

            today_str = time.strftime("%Y-%m-%d", currenttime_time)
            date_datetime = datetime.datetime.strptime(today_str, '%Y-%m-%d')
            DB_switch_datetime  = date_datetime - datetime.timedelta(hours=15)

            CurrentTable_all = "object_list_all"
            # IMAGE_path_table = "image_info"
            if utc_datetime >= DB_switch_datetime:
                CurrentTable = "object_running_list_current"
            else:
                CurrentTable = "object_running_list_history"

            print(utc_datetime,DB_switch_datetime,CurrentTable)
            
            op_time = []
            unit_ID = []
            Grid_ID = []
            Field_ID = []
            Pointing_RA = []
            Pointing_DEC = []
            op_sn = []
            Group_ID = []
            obj_sour = []
            obj_name = []
            obstype =[]
            priority = []
            mag_gap = 0.75        
            for m in range(len(ID_external_trigger_arr)):
                query = ("SELECT " + CurrentTable + ".op_time," + CurrentTable + ".unit_id," + CurrentTable + ".grid_id," \
                    + CurrentTable + ".field_id,"  + CurrentTable + ".ra," + CurrentTable + ".dec," + \
                    CurrentTable + ".op_sn," + CurrentTable + ".group_id," + CurrentTable_all + ".objsour," + CurrentTable_all + ".obj_name," \
                    + CurrentTable_all + ".obs_type," + CurrentTable_all + ".priority" + \
                    " from " + CurrentTable + "," + CurrentTable_all +  \
                    " where ( " + CurrentTable_all+".group_id='XL001'" + \
                    " and " + CurrentTable_all + ".objsour like \'%" + str(ID_external_trigger_arr[m]) + "%\' " + \
                    " and ( " + CurrentTable_all + ".obs_type = \'toa\' or " + CurrentTable_all + ".obs_type = \'tom\' ) " + \
                    " and " + CurrentTable + ".obj_id = " + CurrentTable_all + ".obj_id" + \
                    ") ")
                # print(query)

                db = Yunwei_DBConnect(location)
                cursor = db.cursor()

                cursor.execute(query)
                rows = cursor.fetchall()
                # print(rows)
                if len(rows) > 1:
                    for row in rows:
                    # if utc_datetime_begin_str_T <= row[0] <= utc_datetime_end_str_T:
                        op_time.append(row[0])
                        unit_ID.append(row[1])
                        Grid_ID.append(row[2])
                        Field_ID.append(row[3])
                        Pointing_RA.append(row[4])
                        Pointing_DEC.append(row[5])
                        op_sn.append(row[6])
                        Group_ID.append(row[7])
                        obj_sour.append(row[8])
                        obj_name.append(row[9])
                        obstype.append(row[10])
                        priority.append(row[11])
                else:
                    print('no record returns')
                Yunwei_DBClose(db)

        u,indices = np.unique(Field_ID, return_index=True)
        # print(utc_time_str,u,indices)

        if len(u) > 0:
            # for nin in indices:
            #     print(op_time[nin],unit_id[nin],Grid_ID[nin],Field_ID[nin],Pointing_RA[nin],Pointing_DEC[nin])
            #print("I,op_sn, Group_ID, Unit_ID, Grid_ID, Field_ID, RA, DEC, trigger_name, B_UT , E_UT, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE, FoV, Limit Mag, obj_sour, obj_name, obstype, priority, image_path")  
            
            q = 1
            for nin in indices:
                # utc_datetime_str = "2018-04-28 00:00:00"
                # utc_datetime = datetime.datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
                # utc_datetime_begin_str = "2018-04-28 17:57:00"
                # utc_datetime_begin = datetime.datetime.strptime(utc_datetime_begin_str, '%Y-%m-%d %H:%M:%S')
                # utc_datetime_end_str = "2018-04-28 18:00:00"
                # utc_datetime_end = datetime.datetime.strptime(utc_datetime_end_str, '%Y-%m-%d %H:%M:%S')
                # Pointing_RA[nin] = 233.87
                # Pointing_DEC[nin] = 74.5
                data = func_gwac_too_image_status_query(configuration_file,utc_datetime,utc_datetime_begin,utc_datetime_end,float(Pointing_RA[nin]),float(Pointing_DEC[nin]))
                if len(data[0]) >= 1:         
                    all_list = first_and_last_image(data)[0] 
                    mark_list = first_and_last_image(data)[1]

                    limit_mag_array = []
                    # print(all_list)
                    for k in all_list:
                        if k['FWHM'] > 0 and k['FWHM'] <= 10 and k['S2N'] >= 1.0:
                            limit_mag_array.append(k['LIMIT_MAG'])
                            # print(k['LIMIT_MAG'])
                    limit_mag_array = np.array(limit_mag_array)

                    limit_mag = 0.0
                    if len(limit_mag_array) > 0:
                        limit_mag = np.mean(limit_mag_array) - mag_gap

                    p = 0
                    for k in all_list:
                        B_UT = k['B_UT'].strftime('%Y-%m-%dT%H:%M:%S')
                        E_UT = k['E_UT'].strftime('%Y-%m-%dT%H:%M:%S')
                        M_coor_ra_deg = str(mark_list[p][0])
                        M_coor_dec_deg = str(mark_list[p][1])
                        M_coor_ra = dd2hms(float(M_coor_ra_deg))
                        M_coor_dec = dd2dms(float(M_coor_dec_deg))
                        Image_coor_ra_deg = str(k['Image_RA'])
                        Image_coor_dec_deg = str(k['Image_DEC'])
                        Image_coor_ra = dd2hms(float(Image_coor_ra_deg))
                        Image_coor_dec = dd2dms(float(Image_coor_dec_deg))
                        CCD_ID = str(k['CCD_ID'])
                        CCD_TYPE = str(k['CCD_TYPE'])
                        if CCD_TYPE == "FFOV":
                            fov = "30x30"
                        elif CCD_TYPE == "JFOV":
                            fov = "12.5x12.5"
                        outline = '%i %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %4.2f %s %s %s %s' % (q,op_sn[nin], Group_ID[nin], unit_ID[nin],  Grid_ID[nin], Field_ID[nin], Pointing_RA[nin], Pointing_DEC[nin], trigger_name, B_UT , E_UT, \
                            Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE, fov, limit_mag, obj_sour[nin], obj_name[nin], obstype[nin], priority[nin])
                        # print(outline)
                        
                        finally_result.append(outline)
                        # print(finally_result)
                        p += 1
                        q += 1
    return finally_result
# print(get_gwac_trigger_info())