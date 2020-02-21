"""

   Pubsub envelope subscriber   
 
   Author: Xuhui Han
  
"""
import sys
# from jdcal import gcal2jd
import os
from os.path import exists, join
import os.path
from os import pathsep
import datetime
import time
# load the adapter
import psycopg2
# load the psycopg extras module
import psycopg2.extras
try:
    sys.path.append("./jdcal/")
    from jdcal import gcal2jd
    from jdcal import jd2gcal
except:
    print("please install jdcal code ")
    sys.exit()
from func_coor_obs_timewindow_calculate import func_coor_obs_timewindow_calculate #``
import numpy as np
import ephem
from datetime import timedelta
from astropy.table import Table,Column
import pandas as pd
import io

# sys.path.append('object_generator')
# from object_generator import obj_insert
# from communication_client import Client

def Map(Func, List):
    if sys.version_info.major == 2:
        new = map(Func, List)
    else:
        new = []
        for i in map(Func, List):
            new.append(i)
    return new

def object_obs_timewindow(location,current_utc_datetime):
    current_utc_datetime = datetime.datetime.utcnow()
    homedir = os.path.realpath(__file__).rsplit('/', 1)[0] + '/'
    if location == 'beijing':
        configuration_file = homedir + 'configuration_bj.dat'
    elif location == 'xinglong':
        configuration_file = homedir + 'configuration_xl.dat'    

    configuration_file_dev = open(configuration_file,'r')

    lines1=configuration_file_dev.read().splitlines()
    configuration_file_dev.close()

    for line1 in lines1:
        word=line1.split()
        # print word
        if word[0] == 'db_host':
            db_host = word[2]
        elif word[0] == 'db_port':
            db_port = word[2]
        elif word[0] == 'db_user':
            db_user = word[2]
        elif word[0] == 'db_passwd':
            db_passwd = word[2]
        elif word[0] == 'db_db':
            db_db = word[2]

    # read telescope pointing constrain data -----------------------------------------
    tele_point_table = homedir + "tele_pointing_constrain.csv"
    tele_pointing_constrain_dframe_all = pd.read_csv(tele_point_table)

    conf_obs_parameters_sys = homedir + 'conf_obs_parameters_sys.dat'
    conf_obs_parameters_sys_dev = open(conf_obs_parameters_sys,'rU')

    conf_obs_parameters=conf_obs_parameters_sys_dev.read().splitlines()
    conf_obs_parameters_sys_dev.close()

    # set observatory parameters ----------------------------------------
    observatory = ephem.Observer()

    object_list = "object_list_current"
    object_list_all = "object_list_all"
    query_cmd = ("SELECT " + \
        object_list + ".obj_id, " + \
        # object_list + ".tw_begin , " + \
        # object_list + ".tw_end , " + \
        # object_list + ".obs_stag , " + \
        object_list_all + ".objra , " + \
        object_list_all + ".objdec , " + \
        object_list_all + ".group_id,  " + \
        object_list_all + ".unit_id,  " + \
        object_list_all + ".priority" + \
        #``
        "  from " + object_list + "," + \
        object_list_all + \
        " where (" + \
        "(" + \
        object_list + ".obs_stag = \'initial\' and " + \
        # "( " + object_list_all + ".priority < 90 and " + \
        # object_list_all + ".priority > 99 ) and " + \
        # object_list + ".mode != \'test\'" + \
        # ") and (" + \
        object_list + ".obj_id = " + \
        object_list_all + ".obj_id " + \
        # ") and (" + \
        # object_list_all + ".mode != \'test\'" + \
        ")"
        ")" )

    current_date = current_utc_datetime.strftime("%Y/%m/%d")

    try:
        conn=psycopg2.connect("host=" + db_host + " port= " + db_port + \
            " dbname='" + db_db + "' user='" + db_user + "' password='"  + db_passwd + "'")
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query_cmd) # retrive the object list
        row = cur.fetchall()
        cur.close()

        rows_np = np.array(row)
        rows_table = Table(rows_np, names=('obj_id', 'objra', 'objdec','group_id','unit_id','priority'),dtype=('a19','f','f','a19','a19','i'))

        table_count = len(rows_table)
        data_io = io.StringIO()
        if len(row) > 0:
            Op_time = current_utc_datetime.strftime( '%Y-%m-%d  %H:%M:%S' )  
            Op_time = time.strptime( Op_time, "%Y-%m-%d  %H:%M:%S") 
            gcal_y = Op_time.tm_year
            gcal_m = Op_time.tm_mon
            gcal_d = Op_time.tm_mday
            gcal_hh = Op_time.tm_hour
            gcal_mm = Op_time.tm_min
            gcal_ss = Op_time.tm_sec
            gcal_dd = (gcal_hh/24.0)+(gcal_mm/24.0/60.0)+(gcal_mm/24.0/60.0/60.0)
            MJD_current = gcal2jd(gcal_y,gcal_m,gcal_d)[1] 
            MJD_time_current = MJD_current + gcal_dd
            # print(MJD_current,MJD_time_current)
            date_current = jd2gcal(2400000.5, MJD_current)
            calendar_d_lable = "%d_%d_%d" % (date_current[0],date_current[1],date_current[2])
            calendar_d = "%d-%d-%d" % (date_current[0],date_current[1],date_current[2])

            data = func_coor_obs_timewindow_calculate(rows_table['obj_id'],rows_table['group_id'],rows_table['unit_id'],rows_table['objra'],rows_table['objdec'],rows_table['priority'],MJD_time_current,tele_pointing_constrain_dframe_all,conf_obs_parameters,observatory)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            for i in range(len(data)):
                update_cmd = ("update \"" + object_list + "\" set " + \
                    "tw_begin =\'" + data[i]['tw_begin'] + "\'," + \
                    "tw_end =\'" + data[i]['tw_end'] + "\'," + \
                    "obs_stag =\'" + data[i]['obs_stag'] + "\' " + \
                    "where obj_id=\'" + data[i]['obj_id'] + "\'" )
                cur.execute(update_cmd) # input observation time window and obs stage
            conn.commit()
            cur.close()

    except Exception as e:
        print('Error %s' % e ) 
    finally:
        conn.close()

if __name__ == '__main__':
    location = 'xinglong'
    # location = 'beijing'
    current_utc_datetime = datetime.datetime.utcnow()
    object_obs_timewindow(location,current_utc_datetime)





