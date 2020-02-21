# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
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
import time
from time import gmtime, strftime,localtime
import json
import psycopg2
import psycopg2.extras
import numpy as np
from GWAC_field_load import GWAC_field_load
from func_gwac_too_fieldmatch_file import func_gwac_too_fieldmatch_file
from angular_distance import angular_distance

#__________________________________
def Yunwei_DBConnect(location): # 打开数据库连接
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open(os.path.realpath(__file__).rsplit('/',1)[0] + "/db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        yunwei_host = data["yunwei_host_bj"]
    elif location == 'xinglong':
        yunwei_host = data["yunwei_host_xl"]
    db=psycopg2.connect("host=" + yunwei_host + " port= " + data["yunwei_port"] + \
        " dbname='" + data["yunwei_db"] + "' user='" + data["yunwei_user"] + "' password='"  + data["yunwei_password"] + "'")
    return db

#__________________________________
def Yunwei_DBClose(db): # 关闭数据库连接
    "Close the connection to the DB"
    db.close()

#__________________________________
def check_assignment_request(db): # 检查排在最前面的目标是否可用60cm 和30cm观测
    object_list = "object_list_current"
    object_list_all = "object_list_all"
    query_cmd = ("SELECT " + \
    	object_list + ".id, " + \
        object_list + ".obj_id, " + \
        object_list_all + ".group_id,  " + \
        object_list_all + ".unit_id,  " + \
        object_list_all + ".obs_type,  " + \
        object_list_all + ".priority,  " + \
        object_list_all + ".objrank,  " + \
        object_list_all + ".mode  " + \
        "  from " + object_list + "," + \
        object_list_all + \
        " where (" + \
        "(" + \
        object_list_all + ".group_id = \'XL002|XL003\' " + \
        ") and (" + \
        object_list + ".obs_stag = \'scheduled\' " + \
        # " and " + \
        # object_list + ".mode = \'test\'" + \
        ") and (" + \
        object_list + ".obj_id = " + \
        object_list_all + ".obj_id " + \
        # ") " + \
        # "and (" + \
        # # object_list_all + ".mode != \'test\'" + \
        # # ")  and (" + \
        # object_list + ".obs_stag = \'complete\'" + \
        ") " + \
        ") ORDER BY " + \
        object_list + ".id LIMIT 1"
        "" )
    print(query_cmd)
    assignment_request = 0
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query_cmd)
        rows = cursor.fetchall()
        print(rows)
        if len(rows) > 0:
            assignment_request = 1
    except Exception as e:
        print('Error %s' % e ) 

    return assignment_request


#__________________________________
def Select_object(db): # 读取全部可用60cm 和30cm观测的目标
    object_list = "object_list_current"
    object_list_all = "object_list_all"
    query_cmd = ("SELECT " + \
        object_list_all + ".id, " + \
        object_list_all + ".obj_id, " + \
        object_list_all + ".obj_name , " + \
        object_list_all + ".observer , " + \
        object_list_all + ".objsour , " + \
        object_list_all + ".objra , " + \
        object_list_all + ".objdec , " + \
        object_list_all + ".objepoch , " + \
        object_list_all + ".objerror , " + \
        object_list_all + ".group_id,  " + \
        object_list_all + ".unit_id,  " + \
        object_list_all + ".obs_type,  " + \
        object_list_all + ".obs_stra,  " + \
        object_list_all + ".date_beg,  " + \
        object_list_all + ".date_end,  " + \
        object_list_all + ".day_int,  " + \
        object_list_all + ".imgtype,  " + \
        object_list_all + ".filter,  " + \
        object_list_all + ".expdur,  " + \
        object_list_all + ".delay,  " + \
        object_list_all + ".frmcnt,  " + \
        object_list_all + ".priority,  " + \
        object_list_all + ".run_name,  " + \
        object_list_all + ".note,  " + \
        object_list_all + ".obs_stag as obs_stag1 ,  " + \
        object_list_all + ".mode,  " + \
        object_list_all + ".objrank , " + \
        object_list + ".obs_stag  " + \
        "  from " + object_list + \
        "," + \
        object_list_all + \
        " where (" + \
        "(" + \
        object_list + ".obs_stag = \'scheduled\'" + \
        # " and " + \
        # # object_list + ".mode = \'observation\'" + \
        # object_list + ".mode = \'test\'" + \
        ") and (" + \
        object_list_all + ".group_id = \'XL002|XL003\' " + \
        ") and (" + \
        object_list + ".obj_id = " + \
        object_list_all + ".obj_id " + \
        ")"
        ")" 
        " ORDER BY " + \
        object_list_all + ".id "
        # " DESC"
        )

    # print(query_cmd)
    # id_array = []
    # obs_id_array = []
    # obj_name_array = []
    # observer_array = []
    # objsour_array = []
    # obs_ra_array = []
    # obs_dec_array = []
    # objepoch_array = []
    # objerror_array = []
    # objrank_array = []
    # group_id_array = []
    # unit_id_array = []
    # obstype_array = []
    # obs_strat_array = []
    # date_beg_array = []
    # date_end_array = []
    # day_int_array = []
    # imgtype_array = []
    # filter_array = []
    # expdur_array = []
    # delay_array = []
    # frmcnt_array = []
    # priority_array = []
    # run_name_array = []
    # mode_array = []
    # obs_stag_array = []
    all_data = []
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print query_cmd
    cursor.execute(query_cmd)
    rows = cursor.fetchall()
    if len(rows) > 0:
        for row in rows:
            print('Select_object',row['obs_stag'],row['group_id'] )
            
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query_cmd)
        rows = cursor.fetchall()
        if len(rows) > 0:
            for row in rows:
                if (row['obs_stag'] == 'scheduled') and (row['group_id'] == 'XL002|XL003'):
                    # id_array.append(row['id'])
                    # obs_id_array.append(row['obj_id'])
                    # obj_name_array.append(row['obj_name'])
                    # observer_array.append(row['observer'])
                    # objsour_array.append(row['objsour'])
                    # obs_ra_array.append(row['objra'])
                    # obs_dec_array.append(row['objdec'])
                    # objepoch_array.append(row['objepoch'])
                    # objrank_array.append(row['objrank'])
                    # group_id_array.append(row['group_id'])
                    # unit_id_array.append(row['unit_id'])
                    # obstype_array.append(row['obstype'])
                    # obs_strat_array.append(row['obs_strat'])
                    # date_beg_array.append(row['date_beg'])
                    # date_end_array.append(row['date_end'])
                    # day_int_array.append(row['day_int'])
                    # imgtype_array.append(row['imgtype'])
                    # filter_array.append(row['filter'])
                    # expdur_array.append(row['expdur'])
                    # delay_array.append(row['delay'])
                    # frmcnt_array.append(row['frmcnt'])
                    # priority_array.append(row['priority'])
                    # run_name_array.append(row['run_name'])
                    # mode_array.append(row['mode'])
                    # obs_stag_array.append(row['obs_stag'])
                    all_data.append(row)
                else:    
                    break
                    # print(row['id'],row['objrank'],row['group_id'],row['mode'])          
    except Exception as e:
        print('Error %s' % e ) 
    # data = np.array([id_array,obs_id_array,obs_ra_array,obs_dec_array,obs_rank_array,obj_name_array,obj_sour_array,obs_timewindow_begin_array,obs_timewindow_end_array,obs_complete_stage_array,group_id_array,unit_id_array,obs_type_array,priority_array,mode], ndmin=2)
    all_data = np.array(all_data)
    return all_data


def telescope_availablily(): # 望远镜可用状态
    # group_ids = ['XL002|XL003','001|001']
    if F60A_status == 1:
        F60A_mark = 100
    if F60B_status == 1:
        F60B_mark = 10
    if F30_status == 1:
        F30_mark = 1
    telescope_status = F60A_mark + F60B_mark + F30_mark
    return (telescope_status)

#__________________________________
def Obs_parameter(json_file): # 读取不同望远镜的观测参数，暂不需要
    "The observation parameters in the static file db_param.json."
    with open(json_file, "r") as read_file:
        data = json.load(read_file)
    # obs_param = [data[telescope]["ObjType"],data[telescope]["Obsprog"],data[telescope]["Obs_type"],data[telescope]["Obs_Strat"],data[telescope]["day_int"],data[telescope]["ImgType"],data[telescope]["filter"],data[telescope]["expdur"],data[telescope]["frmcnt"],data[telescope]["priority"]]
    return data

def F30_galaxy_user_grid(glade_id,ra,dec,score): # 根据输入的星系计算 F30 的天区

    glade_id = np.array(glade_id)
    ra = np.array(ra)
    dec = np.array(dec)
    score = np.array(score)

    # read F30 grid
    grid_id = 'G0015'
    grid_data_array = GWAC_field_load(grid_id)
    file_grid_id_arr = []
    file_field_id_arr = []
    file_fov_sizex_arr = [] 
    file_fov_sizey_arr = []
    file_ra_center_arr = []
    file_dec_center_arr = []
    file_decdeg_h1_arr = []
    file_decdeg_l1_arr = []
    for n in range(len(grid_data_array[0])):
        file_grid_id_arr.append(grid_data_array[1][n])
        file_field_id_arr.append(grid_data_array[2][n])
        file_fov_sizex_arr.append(float(grid_data_array[3][n]))
        file_fov_sizey_arr.append(float(grid_data_array[4][n]))
        file_ra_center_arr.append(float(grid_data_array[5][n]))
        file_dec_center_arr.append(float(grid_data_array[6][n]))
        file_decdeg_h1_arr.append(float(grid_data_array[8][n]))
        file_decdeg_l1_arr.append(float(grid_data_array[12][n]))
    # print(file_field_id_arr)
    glade_id_arr = []
    probability_arr = []
    grid_id_arr = []
    field_id_arr = []
    fov_sizex_arr = [] 
    fov_sizey_arr = []    
    ra_center_arr = []
    dec_center_arr = []
    for n in range(len(glade_id)):
        # print(ra[n],dec[n],grid_id,file_field_id_arr,file_fov_sizex_arr,file_ra_center_arr)
        radeg = float(ra[n])
        decdeg = float(dec[n])      
        data = func_gwac_too_fieldmatch_file(radeg,decdeg,0.01,grid_id,file_field_id_arr,file_fov_sizex_arr,file_fov_sizey_arr,file_ra_center_arr,file_dec_center_arr,file_decdeg_h1_arr,file_decdeg_l1_arr)
        if len(data[0]) > 0:
            glade_id_arr.append(glade_id[n]) 
            probability_arr.append(score[n])
            grid_id_arr.append(data[1][0])
            field_id_arr.append(data[2][0])
            # fov_sizex_arr.append(data[3][0])
            # fov_sizey_arr.append(data[4][0])
            ra_center_arr.append(data[5][0])
            dec_center_arr.append(data[6][0])
    print('field_match_done')

    # probability sum
    glade_id_nparr = np.array(glade_id_arr)
    grid_id_nparr = np.array(grid_id_arr)
    field_id_nparr = np.array(field_id_arr)
    fov_sizex_nparr = np.array(fov_sizex_arr)
    # fov_sizey_nparr = np.array(fov_sizey_arr)
    ra_center_nparr = np.array(ra_center_arr)
    dec_center_nparr = np.array(dec_center_arr)
    probability_nparr = np.array(probability_arr).astype(np.float)
    field_id_arr_uniq=np.unique(field_id_nparr)
    field_id_index=np.unique(field_id_nparr, return_index=True)
    print('type',type(probability_nparr[0]))
    glade_id_sum = []
    grid_id_sum = []
    field_id_sum = []
    # fov_sizex_sum = []
    # fov_sizey_sum = []
    ra_center_sum = []
    dec_center_sum = []
    probability_sum = []

    for m in range(len(field_id_index[0])):
        glade_index_one_field = np.where(field_id_nparr == field_id_nparr[field_id_index[1][m]])[0]
        s = "|"
        glade_id_sum.append(s.join(glade_id_nparr[glade_index_one_field]))
        grid_id_sum.append(grid_id_nparr[field_id_index[1][m]])
        field_id_sum.append(field_id_nparr[field_id_index[1][m]])
        # fov_sizex_sum.append(fov_sizex_nparr[field_id_index[1][m]])
        # fov_sizey_sum.append(fov_sizey_nparr[field_id_index[1][m]])
        ra_center_sum.append(ra_center_nparr[field_id_index[1][m]])
        dec_center_sum.append(dec_center_nparr[field_id_index[1][m]])
        probability_sum.append(sum(probability_nparr[np.where(field_id_nparr == field_id_index[0][m])]))
    

    index_sort = np.argsort(probability_sum)[::-1]
    glade_id_sum_new = []
    grid_id_sum_new = []
    field_id_sum_new = []
    # fov_sizex_sum_new = [] 
    # fov_sizey_sum_new = []    
    ra_center_sum_new = []
    dec_center_sum_new = []
    probability_sum_new = []
    for p in range(len(field_id_sum)):
        glade_id_sum_new.append(glade_id_sum[index_sort[p]])
        grid_id_sum_new.append(grid_id_sum[index_sort[p]])
        field_id_sum_new.append(field_id_sum[index_sort[p]])
        # fov_sizex_sum_new.append(fov_sizex_sum[index_sort[p]])
        # fov_sizey_sum_new.append(fov_sizey_sum[index_sort[p]])
        ra_center_sum_new.append(ra_center_sum[index_sort[p]])
        dec_center_sum_new.append(dec_center_sum[index_sort[p]])
        probability_sum_new.append(probability_sum[index_sort[p]])
    
    return(grid_id_sum_new,field_id_sum_new,ra_center_sum_new,dec_center_sum_new,probability_sum_new,glade_id_sum_new)

def F60_F30_galaxy_assign(data,telescope_status): # 对星系分配望远镜
    # id_array,obs_id_array,obs_ra_array,obs_dec_array,obs_rank_array,obj_name_array,obj_sour_array,obs_timewindow_begin_array,obs_timewindow_end_array,obs_complete_stage_array,group_id_array,unit_id_array,obs_type_array,priority_array,mode
    n_F60_block = 7
    n_F30_block = 5
    data_F60A = []
    data_F60B = []
    data_F30 = []
    object_num = len(data)
    print("object_num",object_num)
    if object_num > 0:
        if (telescope_status in [111,110] and object_num >= (n_F60_block*2))  :

            data_F60A = data[np.arange(0,((n_F60_block-1)*2),2)]
            data_F60B = data[np.arange(1,((n_F60_block-1)*2),2)]
            data_rest = data[10:]
            print('case1')
        elif (telescope_status in [111,110] and object_num < (n_F60_block*2))  :
            for n in range(object_num):
                if n in np.arange(0,((n_F60_block-1)*2),2):
                    data_F60A.append(data[n])
                elif n in np.arange(1,((n_F60_block-1)*2),2):
                    data_F60B.append(data[n])
            data_rest = data[n+1:]
            print('case2')
        elif (telescope_status in [101,100] and object_num >= n_F60_block)  :
            data_F60A = data[np.arange(0,(n_F60_block-1),1)]
            data_rest = data[5:]
            print('case3')
        elif (telescope_status in [11,10] and object_num >= n_F60_block)  :
            data_F60B = data[np.arange(0,(n_F60_block-1),1)]
            data_rest = data[5:]
            print('case4')
        elif (telescope_status in [101,100] and object_num < n_F60_block)  :
            for n in range(object_num):
                data_F60A.append(data[n])
            data_rest = []
            print('case5')
        elif (telescope_status in [11,10] and object_num < n_F60_block)  :
            for n in range(object_num):
                data_F60B.append(data[n])
            data_rest = []
            print('case6')
        elif (telescope_status in [1])  :
            data_rest = data
            print('case7')
        if (telescope_status in [1,11,101,111] and len(data_rest) > 0)  :

            grid_id_sum_new,field_id_sum_new,ra_center_sum_new,dec_center_sum_new,probability_sum_new,glade_id_sum_new = F30_galaxy_user_grid(data_rest[:,2],data_rest[:,5],data_rest[:,6],data_rest[:,26])
            # data_grid = np.array([grid_id_sum_new,field_id_sum_new,ra_center_sum_new,dec_center_sum_new,probability_sum_new,glade_id_sum_new])
            data_F30 = []
            if len(grid_id_sum_new) >= n_F30_block:
                for mx in range(n_F30_block):
                    obj_name = (grid_id_sum_new[mx]+"_"+field_id_sum_new[mx])
                    data_F30.append([0,0,obj_name,data_rest[0,3],data_rest[0,4],ra_center_sum_new[mx],dec_center_sum_new[mx],data_rest[0,7],data_rest[0,8],\
                        'XL003','001',data_rest[0,11],data_rest[0,12],data_rest[0,13],data_rest[0,14],data_rest[0,15],data_rest[0,16],data_rest[0,17],\
                        data_rest[0,18],data_rest[0,19],data_rest[0,20],data_rest[0,21],data_rest[0,22],data_rest[0,23],data_rest[0,24],data_rest[0,25],probability_sum_new[mx],glade_id_sum_new[mx]])
            else:
                for mx in range(len(grid_id_sum_new)):
                    obj_name = (grid_id_sum_new[mx]+"_"+field_id_sum_new[mx])
                    data_F30.append([0,0,obj_name,data_rest[0,3],data_rest[0,4],ra_center_sum_new[mx],dec_center_sum_new[mx],data_rest[0,7],data_rest[0,8],\
                        'XL003','001',data_rest[0,11],data_rest[0,12],data_rest[0,13],data_rest[0,14],data_rest[0,15],data_rest[0,16],data_rest[0,17],\
                        data_rest[0,18],data_rest[0,19],data_rest[0,20],data_rest[0,21],data_rest[0,22],data_rest[0,23],data_rest[0,24],data_rest[0,25],probability_sum_new[mx],glade_id_sum_new[mx]])                
            print('case8')
            data_F30 = np.asarray(data_F30)
    return (data_F60A,data_F60B,data_F30)

def insert_trigger_obj_field_op_talbe(db,data_F30): # 将 trigger ID, F30 观测天区名，星系列表记录到数据库
    object_list = "trigger_obj_field_op_sn"
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)  
        if (len(data_F30) > 0) :
             for data in data_F30:
                trigger_ID = data[4].split('_')[2]
                Serial_num = data[4].split('_')[1]
                insert_cmd = "insert into " + object_list + " values ( DEFAULT , " + \
                        "'" + trigger_ID + "'," + \
                        "'" + Serial_num + "'," + \
                        "'" + data[2] + "'," + \
                        "'" + data[27] + "')"    
                print(insert_cmd)
                cursor.execute(insert_cmd)
                db.commit()
        cursor.close()
    except Exception as e:
        print('Error %s' % e ) 
    return    

def obj_list_all_set_telescope_F60(db,data_F60A,data_F60B): # 对分配给 F60 的观测目标修改数据库
    object_list = "object_list_all"
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)  
        if (len(data_F60A) > 0) :
             for data in data_F60A:
                insert_cmd = "UPDATE " + object_list + " SET group_id = " + \
                        "'XL002'," + \
                        " unit_id = '001' , " + \
                        " priority = " + str(int(data[21])+1) + "" + \
                        " WHERE ( obj_id = " + \
                        "'" + str(data[1]) + "')" 
                print(insert_cmd)   
                cursor.execute(insert_cmd)
                db.commit()
        if (len(data_F60B) > 0) :
             for data in data_F60B:
                insert_cmd = "UPDATE " + object_list + " SET group_id = " + \
                        "'XL002'," + \
                        " unit_id = '002' , " + \
                        " priority = " + str(int(data[21])+1) + "" + \
                        " WHERE ( obj_id = " + \
                        "'" + str(data[1]) + "')"   
                print(insert_cmd) 
                cursor.execute(insert_cmd)
                db.commit()     

        cursor.close()
    except Exception as e:
        print('Error %s' % e ) 
    return  


def insert_obj_list_all_telescope_F30(db,data_F30): # 将分配给 F30的天区，录入到观测目标总表
    object_list = "object_list_all"
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)  
        if (len(data_F30) > 0) :
             for data in data_F30:
                Obs_Strat = "dithering"
                ra_shift = "0.005"
                dec_shift = "0.0"
                insert_cmd = "insert into " + object_list + \
                    "( id, " + \
                    "obj_id, " + \
                    "obj_name , " + \
                    "observer , " + \
                    "objsour , " + \
                    "objra , " + \
                    "objdec , " + \
                    "objepoch , " + \
                    "objerror , " + \
                    "group_id,  " + \
                    "unit_id,  " + \
                    "obs_type,  " + \
                    "obs_stra,  " + \
                    "date_beg,  " + \
                    "date_end,  " + \
                    "day_int,  " + \
                    "imgtype,  " + \
                    "filter,  " + \
                    "expdur,  " + \
                    "delay,  " + \
                    "frmcnt,  " + \
                    "priority,  " + \
                    "run_name,  " + \
                    "note,  " + \
                    "mode,  " + \
                    "obs_stag,  " + \
                    "objrank, " + \
                    "ra_shift,  " + \
                    "dec_shift ) " + \
                    " values ( DEFAULT , " + \
                    " DEFAULT, " + \
                    "'" + str(data[2]) + "'," + \
                    "'" + str(data[3]) + "'," + \
                    "'" + str(data[4]) + "'," + \
                    "" + str(data[5]) + "," + \
                    "" + str(data[6]) + "," + \
                    "" + str(data[7]) + "," + \
                    "'" + str(data[8]) + "'," + \
                    "'" + str(data[9]) + "'," + \
                    "'" + str(data[10]) + "'," + \
                    "'" + str(data[11]) + "'," + \
                    "'" + Obs_Strat + "'," + \
                    "'" + str(data[13]) + "'," + \
                    "'" + str(data[14]) + "'," + \
                    "" + str(data[15]) + "," + \
                    "'" + str(data[16]) + "'," + \
                    "'" + str(data[17]) + "'," + \
                    "'" + str(data[18]) + "'," + \
                    "'" + str(data[19]) + "'," + \
                    "" + str(data[20]) + "," + \
                    "" + str(data[21]+1) + "," + \
                    "" + str(data[22]) + "," + \
                    "'" + str(data[23]) + "'," + \
                    "'" + str(data[25]) + "'," + \
                    "'" + str(data[24]) + "'," + \
                    "'" + str(data[26]) + "'," + \
                    "" + ra_shift + "," + \
                    "" + dec_shift + \
                    ")"  
                print(insert_cmd)

                cursor.execute(insert_cmd)
                db.commit()
        cursor.close()
    except Exception as e:
        print('Error %s' % e ) 
    return  

def update_object_list_current_obs_stag_F30(db,data_F30): # 对已被 F30天区覆盖的星系在数据库中进行标记，将 obs_stag 标记为“cover”
    object_list = "object_list_all"
    object_list_current = "object_list_current"
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)  
        if (len(data_F30) > 0) :
             for data in data_F30:
                glade_id_arr = data[27].split("|")
                for i in glade_id_arr:
                    insert_cmd = "UPDATE " + object_list + " SET obs_stag = " + \
                            "'covered' " + \
                            " WHERE ( obj_name = " + \
                            "'" + i + "' ) "  
                    print(insert_cmd)   
                    cursor.execute(insert_cmd)
                    db.commit()
                    query_cmd = "SELECT obj_id from " + object_list + \
                            " WHERE ( obj_name = " + \
                            "'" + i + "' ) "  
                    print(query_cmd)   
                    cursor.execute(query_cmd)
                    rows = cursor.fetchall()
                    print(rows)   
                    for row in rows:
                        obj_id = row['obj_id']
                        insert_cmd = "UPDATE " +  object_list_current + " SET  obs_stag = " + \
                                "'covered' " + \
                                " WHERE ( obj_id = '" + obj_id + "')" 
                        print(insert_cmd)   
                        cursor.execute(insert_cmd)
                        db.commit()     

        cursor.close()
    except Exception as e:
        print('Error %s' % e ) 

 
    return 

        # object_list_all + ".id, " + \0 int
        # object_list_all + ".obj_id, " + \1
        # object_list_all + ".obj_name , " + \2
        # object_list_all + ".observer , " + \3
        # object_list_all + ".objsour , " + \4
        # object_list_all + ".objra , " + \5 float
        # object_list_all + ".objdec , " + \6 float
        # object_list_all + ".objepoch , " + \7 int
        # object_list_all + ".objerror , " + \8
        # object_list_all + ".group_id,  " + \9
        # object_list_all + ".unit_id,  " + \10
        # object_list_all + ".obs_type,  " + \11
        # object_list_all + ".obs_stra,  " + \12
        # object_list_all + ".date_beg,  " + \13
        # object_list_all + ".date_end,  " + \14
        # object_list_all + ".day_int,  " + \15 int
        # object_list_all + ".imgtype,  " + \16
        # object_list_all + ".filter,  " + \17
        # object_list_all + ".expdur,  " + \18 float
        # object_list_all + ".delay,  " + \19 float
        # object_list_all + ".frmcnt,  " + \20 int
        # object_list_all + ".priority,  " + \21 int
        # object_list_all + ".run_name,  " + \22 int
        # object_list_all + ".note,  " + \ 23
        # object_list_all + ".mode,  " + \24
        # object_list_all + ".obs_stag as obs_stag1,  " + \25
        # object_list_all + ".objrank , " + \ 26
        # object_list + ".obs_stag  " + \27

                # cursor.execute(insert_cmd)
                # conn.commit()



def main():
    db = Yunwei_DBConnect('xinglong')
    assignment_request = check_assignment_request(db)
    print(assignment_request)
    Yunwei_DBClose(db)
    if assignment_request == 1:
        db = Yunwei_DBConnect('xinglong')
        data = Select_object(db)
        # print('data',data)
        # json_file = './obs_param.json'
        # obs_param = Obs_parameter(json_file)
        # telescope = 'F60-F30'
        # Observer = obs_param[telescope]["Observer"]
        Yunwei_DBClose(db)

        data_F60A,data_F60B,data_F30 = F60_F30_galaxy_assign(data,111)
        print('galaxy data F60A',data_F60A)
        print('galaxy data F60B',data_F60B)
        print('galaxy data F30',data_F30)
        db = Yunwei_DBConnect('xinglong')
        insert_trigger_obj_field_op_talbe(db,data_F30)
        obj_list_all_set_telescope_F60(db,data_F60A,data_F60B)
        insert_obj_list_all_telescope_F30(db,data_F30)
        update_object_list_current_obs_stag_F30(db,data_F30)
        Yunwei_DBClose(db)

        if len(data_F30) > 0:
            return True
        else:
            return False

    return False

main()
