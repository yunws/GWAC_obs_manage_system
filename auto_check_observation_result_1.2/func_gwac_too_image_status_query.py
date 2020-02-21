# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: Xuhui Han
"""
import sys, stat
import math
import subprocess as subp
## {{{ http://code.activestate.com/recipes/52224/ (r1)
from os.path import exists, join
import os.path
from os import pathsep
import string
import os
import datetime
import time
import re
# load the adapter
import psycopg2

# load the psycopg extras module
import psycopg2.extras

def func_gwac_too_image_status_query(configuration_file,utc_datetime,datet_now_start,datet_now_end,mount_ra,mount_dec):

    configuration_file_dev = open(configuration_file,'rU')

    lines1=configuration_file_dev.read().splitlines()
    configuration_file_dev.close()

    for line1 in lines1:
        word=line1.split()
        # print word
        if word[0] == 'gwac_db_server_host':
            gwac_db_server_host = word[2]
        elif word[0] == 'gwac_db_server_port':
            gwac_db_server_port = word[2]
        elif word[0] == 'gwac_db_server_db_name':
            gwac_db_server_db_name = word[2]
        elif word[0] == 'gwac_db_server_user':
            gwac_db_server_user = word[2]
        elif word[0] == 'gwac_db_server_password':
            gwac_db_server_password = word[2]


    datet_now_start_str = datetime.datetime.strftime(datet_now_start, '%Y-%m-%d %H:%M:%S')
    datet_now_end_str = datetime.datetime.strftime(datet_now_end, '%Y-%m-%d %H:%M:%S')

    currenttime_time  = time.gmtime()
    currenttime_str = time.strftime("%Y-%m-%d %H:%M:%S", currenttime_time)
    currenttime_datetime = datetime.datetime.strptime(currenttime_str, '%Y-%m-%d %H:%M:%S')


    today_str = time.strftime("%Y-%m-%d", currenttime_time)
    date_datetime = datetime.datetime.strptime(today_str, '%Y-%m-%d')
    DB_switch_datetime  = date_datetime - datetime.timedelta(hours=15)
    # print(DB_switch_datetime)

    if utc_datetime < currenttime_datetime and utc_datetime >= DB_switch_datetime:
        gwac_db_server_OT2_table = "ot_level2"
        gwac_db_server_OT_table = "ot_observe_record"
        gwac_db_server_subimage_table = "fits_file_cut"
        gwac_db_server_camera_table = "camera"
        gwac_db_server_img_para_table = "image_status_parameter"
    else:
        gwac_db_server_OT2_table = "ot_level2_his"
        gwac_db_server_OT_table = "ot_observe_record_his"
        gwac_db_server_subimage_table = "fits_file_cut_his"
        gwac_db_server_camera_table = "camera"
        gwac_db_server_img_para_table = "image_status_parameter_his"

    # print(utc_datetime,DB_switch_datetime,gwac_db_server_OT2_table)


    # If we are accessing the rows via column name instead of position we 
    # need to add the arguments to conn.cursor.
    # query_cmd = ("SELECT *  from " + gwac_db_server_img_para_table + \
    #     " where astro_flag = 1  and (time_obs_ut between \'" + \
    #     datet_now_start_str + "\' and \'" + datet_now_end_str + "\')" + \
    #     " and (mount_ra >= " + str(mount_ra-0.01) + " and mount_ra <= " + str(mount_ra+0.01)  + \
    #     " and mount_dec >= " + str(mount_dec-0.01) + " and mount_dec <= " + str(mount_dec+0.01) + ") ")
    query_cmd = ("SELECT " + gwac_db_server_img_para_table + ".* , " + \
        gwac_db_server_camera_table + \
        ".camera_id, " + gwac_db_server_camera_table + \
        ".name from " + gwac_db_server_img_para_table + \
        ", " + gwac_db_server_camera_table + \
        " where " + \
        gwac_db_server_img_para_table + ".dpm_id = " + \
        gwac_db_server_camera_table + ".camera_id " + \
        " and " + \
        gwac_db_server_img_para_table + ".astro_flag = 1 "  # + \
        " and " + \
        "(" + gwac_db_server_img_para_table + ".time_obs_ut between \'" + \
        datet_now_start_str + "\' and \'" + datet_now_end_str + "\')"  + \
        " and (mount_ra >= " + str(mount_ra-0.01) + " and mount_ra <= " + str(mount_ra+0.01)  + \
        " and mount_dec >= " + str(mount_dec-0.01) + " and mount_dec <= " + str(mount_dec+0.01) + ") ")
    # print 'query_cmd',query_cmd

    # Note that below we are accessing the row via the column name.
    datet_array = []
    ff_id_array = []
    mount_ra_array = []
    mount_dec_array = []
    dpm_id_array = []
    camera_id_array = []
    camera_type_array = []
    img_center_ra_array = []
    img_center_dec_array = []
    img_fwhm_array = []
    img_s2n_array = []
    img_avg_limit_array = []
    
    try:
        conn=psycopg2.connect("host=" + gwac_db_server_host + " port= " + gwac_db_server_port + \
            " dbname='" + gwac_db_server_db_name + "' user='" + gwac_db_server_user + "' password='"  + gwac_db_server_password + "'")
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query_cmd)
            rows = cur.fetchall()
            # print rows
            if len(rows) > 1:
                for row in rows:
                    datet_str = str(row["time_obs_ut"])[0:19]
                    datet = datetime.datetime.strptime(datet_str, '%Y-%m-%d %H:%M:%S')
                    datet_array.append(datet)
                    ff_id_array.append(row['ff_id'])
                    mount_ra_array.append(row['mount_ra'])
                    mount_dec_array.append(row['mount_dec'])    
                    dpm_id_array.append(row['dpm_id'])
                    camera_id_array.append(row['name'])
                    if row['name'][2] == '5':
                        camera_type = 'FFOV'
                    else:
                        camera_type = 'JFOV'
                    camera_type_array.append(camera_type)
                    img_center_ra_array.append(row['img_center_ra']) 
                    img_center_dec_array.append(row['img_center_dec'])
                    img_fwhm_array.append(row['fwhm'])
                    img_s2n_array.append(row['s2n'])
                    img_avg_limit_array.append(row['avg_limit'])

                    # print ("%s %i %6.2f %6.2f %i %s %s %8.4f %8.4f" % (str(row["time_obs_ut"])[0:19],row['ff_id'],row['mount_ra'],row['mount_dec'],row['dpm_id'],row['name'],camera_type,row['img_center_ra'],row['img_center_dec']))                                    
                    # g.write("%s %6.2f %6.2f %i %8.4f %8.4f\n" % (str(row["time_obs_ut"])[0:19],row['mount_ra'],row['mount_dec'],row['dpm_id'],row['img_center_ra'],row['img_center_dec']))            

            else:
                print('no too record returns')
        except:
            print("I can't SELECT from image status parameter table")

    except:
        print('Error')
        
    # except psycopg2.DatabaseError, e:
    #     print 'Error %s' % e    
    #     # sys.exit(1)
    #     # continue
         
    finally:
        
        if conn:
            conn.close()

    # print(img_fwhm_array,img_s2n_array)
    return datet_array,mount_ra_array,mount_dec_array,dpm_id_array,img_center_ra_array,img_center_dec_array,camera_id_array,camera_type_array,ff_id_array,img_fwhm_array,img_s2n_array,img_avg_limit_array


# configuration_file =  'configuration_bj.dat'
# utc_datetime_str = "2018-04-23 18:09:48"
# utc_datetime = datetime.datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
# utc_datetime_begin_str = "2018-04-23 18:05:48"
# utc_datetime_begin = datetime.datetime.strptime(utc_datetime_begin_str, '%Y-%m-%d %H:%M:%S')
# utc_datetime_end_str = "2018-04-23 18:14:48"
# utc_datetime_end = datetime.datetime.strptime(utc_datetime_end_str, '%Y-%m-%d %H:%M:%S')
# Pointing_RA = 233.53
# Pointing_DEC = 24.50
# data = func_gwac_too_image_status_query(configuration_file,utc_datetime,utc_datetime_begin,utc_datetime_end,float(Pointing_RA),float(Pointing_DEC))
# print(data)
 
