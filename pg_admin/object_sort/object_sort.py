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
from func_object_sort import func_object_sort #``
import numpy as np
import ephem
from datetime import timedelta
from astropy.table import Table,Column
import pandas as pd
import io
import csv

def Map(Func, List):
    if sys.version_info.major == 2:
        new = map(Func, List)
    else:
        new = []
        for i in map(Func, List):
            new.append(i)
    return new

def object_sort(location,current_utc_datetime):
    op_time = current_utc_datetime.strftime( '%Y/%m/%d %H:%M:%S' )  
    Op_time = time.strptime(op_time, "%Y/%m/%d %H:%M:%S") 
    gcal_y = Op_time.tm_year
    gcal_m = Op_time.tm_mon
    gcal_d = Op_time.tm_mday
    gcal_hh = Op_time.tm_hour
    gcal_mm = Op_time.tm_min
    gcal_ss = Op_time.tm_sec
    gcal_dd = (gcal_hh/24.0)+(gcal_mm/24.0/60.0)+(gcal_mm/24.0/60.0/60.0)
    MJD_current = gcal2jd(gcal_y,gcal_m,gcal_d)[1] 
    MJD_time_current = MJD_current + gcal_dd    

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

    object_list = "object_list_current"
    object_list_all = "object_list_all"
    query_cmd = ("SELECT " + \
        object_list + ".obj_id, " + \
        object_list + ".tw_begin , " + \
        object_list + ".tw_end , " + \
        object_list + ".obs_stag , " + \
        object_list_all + ".objra , " + \
        object_list_all + ".objdec , " + \
        object_list_all + ".objrank , " + \
        object_list_all + ".group_id,  " + \
        object_list_all + ".unit_id,  " + \
        # object_list_all + ".obs_type,  " + \
        object_list_all + ".priority,  " + \
        object_list_all + ".mode,  " + \
        object_list_all + ".insert_time  " + \
        #``
        "  from " + object_list + "," + \
        object_list_all + \
        " where (" + \
        "(" + \
        # object_list + ".obs_stag != \'observable\' and " + \
        # object_list + ".obs_stag != \'removed\' and " + \
        # object_list + ".obs_stag != \'complete\' and " + \
        # object_list + ".mode != \'test\'" + \
        # ") and (" + \
        object_list + ".obj_id = " + \
        object_list_all + ".obj_id " + \
        # ") and (" + \
        # object_list_all + ".mode != \'test\'" + \
        ")"
        ")" )

    truncate_cmd = ("truncate " + object_list ) 

    current_date = current_utc_datetime.strftime("%Y/%m/%d")

    data = []
    try:
        conn=psycopg2.connect("host=" + db_host + " port= " + db_port + \
            " dbname='" + db_db + "' user='" + db_user + "' password='"  + db_passwd + "'")
        # try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query_cmd) # retrive the object list
        row = cur.fetchall()
        cur.close()
        # print('row',row)

        rows_np = np.array(row)
        rows_table = Table(rows_np, names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))
        rows_sent = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))
        rows_old = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))
        rows_scheduled = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))
        rows_pass = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))
        rows_observable = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))
        rows_rest = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag', 'objra', 'objdec','objrank','group_id','unit_id','priority','mode','insert_time'),dtype=('a19','a19','a19','a19', 'f', 'f','f','a19','a19','i','a19','a19'))

        table_count = len(rows_table)
        if len(row) > 0:
            scheduled_count = 0
            sent_count = 0
            rest_count = 0
            observable_count = 0
            pass_count = 0

            sent_index = np.where(rows_table['obs_stag'] == 'sent')[0] # get sent objects
            # print('sent_index',sent_index)
            sent_count = len(sent_index)
            if sent_count > 0:
                rows_sent = rows_table[sent_index]
                rows_table.remove_rows(sent_index)

            old_index = np.where(((rows_table['obs_stag'] == 'scheduled') | (rows_table['obs_stag'] == 'observable')))[0] # get scheduled and observable objects
            # print('scheduled_index',old_index)
            old_count = len(old_index)
            if old_count > 0:
                rows_old = rows_table[old_index]
                rows_table.remove_rows(old_index)
                scheduled_index = np.where(((rows_old['tw_begin'] <= op_time ) & (rows_old['tw_end'] > op_time)) | ((rows_old['priority'] >= 90) & (rows_old['priority'] <= 99)))[0] # get scheduled and observable objects
                rows_scheduled = rows_old[scheduled_index]
                rows_scheduled['obs_stag'] = 'scheduled'
                scheduled_count = len(rows_scheduled)
                rows_old.remove_rows(scheduled_index)
                pass_index = np.where(rows_old['tw_end'] <= op_time)[0]
                rows_pass = rows_old[pass_index]
                rows_pass['obs_stag'] = 'pass'
                pass_count = len(rows_pass)
                rows_old.remove_rows(pass_index)
                rows_observable = rows_old
                rows_observable['obs_stag'] = 'observable'
                observable_count =  len(rows_observable)

            rows_rest = rows_table # get rest objects
            rest_count = len(rows_rest)

            # current_utc_datetime_1 = datetime.datetime.utcnow()
            # print('check time 1', current_utc_datetime_1 - current_utc_datetime)

            data_io = io.StringIO()
            if sent_count > 0: # insert sent object into the table
                for n in range(sent_count):
                    data_io.write(u"""%s,%s,%s,%s,%s,%s\n""" % (rows_sent[n]['obj_id'],current_date,rows_sent[n]['tw_begin'],rows_sent[n]['tw_end'],rows_sent[n]['obs_stag'],rows_sent[n]['mode']))
            
            # current_utc_datetime_1 = datetime.datetime.utcnow()
            # print('check time 4', current_utc_datetime_1 - current_utc_datetime)

            if scheduled_count > 0: # insert scheduled object into the table   

                data = func_object_sort(rows_scheduled, current_utc_datetime)
                            
                for n in range(len(data)): 
                    data_io.write(u"""%s,%s,%s,%s,scheduled,%s\n""" % (data[n]['obj_id'],current_date,data[n]['tw_begin'],data[n]['tw_end'],data[n]['mode']))

            if observable_count > 0: # insert observable objects into the table 
                for n in range(observable_count):
                    data_io.write(u"""%s,%s,%s,%s,observable,%s\n""" % (rows_observable[n]['obj_id'],current_date,rows_observable[n]['tw_begin'],rows_observable[n]['tw_end'],rows_observable[n]['mode']))


            if rest_count > 0: # insert rest objects into the table 
                for n in range(rest_count):
                    data_io.write(u"""%s,%s,%s,%s,%s,%s\n""" % (rows_rest[n]['obj_id'],current_date,rows_rest[n]['tw_begin'],rows_rest[n]['tw_end'],rows_rest[n]['obs_stag'],rows_rest[n]['mode']))

            if pass_count > 0:
                for n in range(pass_count):
                    data_io.write(u"""%s,%s,%s,%s,pass,%s\n""" % (rows_pass[n]['obj_id'],current_date,rows_pass[n]['tw_begin'],rows_pass[n]['tw_end'],rows_pass[n]['mode']))

            # print(data_io.read())
            # current_utc_datetime_1 = datetime.datetime.utcnow()
            # print('check time 2', current_utc_datetime_1 - current_utc_datetime)

            if table_count == 0: #``
                print('no record returns')
            else:
                data_io.seek(0)
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute(truncate_cmd) # empty the current object table
                conn.commit()
                cur.close()

                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                copy_cmd = ("copy \"" + object_list + "\" (obj_id, date_cur, tw_begin, tw_end, obs_stag, mode) from STDIN delimiter ','")
                cur.copy_expert(copy_cmd, data_io)
                conn.commit()
                cur.close()
                print(data_io.read())

            # current_utc_datetime_1 = datetime.datetime.utcnow()
            # print('check time 3', current_utc_datetime_1 - current_utc_datetime)
        conn.close()
        return data[0]['obj_id'] if len(data) else rows_observable[0]['obj_id']

    except Exception as e:
        print('Error %s' % e ) 
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    location = 'xinglong'
    # location = 'beijing'
    current_utc_datetime = datetime.datetime.utcnow()
    # mode = 'observation'
    object_sort(location,current_utc_datetime)
    # current_utc_datetime_1 = datetime.datetime.utcnow()
    # d = (current_utc_datetime_1 - current_utc_datetime)
    # print(d)




