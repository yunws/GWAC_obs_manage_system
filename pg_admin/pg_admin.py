#!/usr/bin/python
# -*- coding: utf-8 -*-
#author: Xiao Yujie
#update: 2019-11-25
#version: 0.0.5

import datetime, sys
from class_pg_admin import pg_admin
from communication_client import Client
sys.path.append('object_obs_timewindow')
from object_obs_timewindow import object_obs_timewindow
sys.path.append('object_sort')
from object_sort import object_sort
sys.path.append('telescope_assign')
from ToP_GW_followup_F60_F30_galaxy_assign import main as telescope_assign

def write_log(logpath, logline):
    log = open(logpath, 'a')
    log.write(str(logline) + '\n')
    log.close()

client = Client(client_type='object_sort')
pgdb_admin = pg_admin()

location = 'xinglong'
mode = 'observation'
last_run_date = ''
object_list_current = 'object_list_current'

while True:
    client.Recv()
    t_b = datetime.datetime.now()
    data_recv = client.recv_data
    current_utc_datetime = datetime.datetime.utcnow()
    current_date = current_utc_datetime.strftime("%Y/%m/%d")
    log_date = current_utc_datetime.strftime("%Y%m%d")
    current_time = current_utc_datetime.strftime("%Y/%m/%d %H:%M:%S")
    print current_time

    logpath = './logs/sort_debug_' + log_date + '.log'

    # 每日初始化current表
    if current_date != last_run_date:
        date_in_pgdb = pgdb_admin.get_date_in_pgdb()
        if not date_in_pgdb or date_in_pgdb and date_in_pgdb != current_date:
            pgdb_admin.current_to_history()
            pgdb_admin.pg_process(pg_action=['truncate', object_list_current])
            pgdb_admin.pg_process(pg_action=['daily_init', object_list_current])
        last_run_date = current_date

    if data_recv['error']:
        continue
    if data_recv['pg_action'][0] == 'update':
        obs_stag = data_recv['content']['obs_stag']
        pgdb_admin.pg_process(data_recv['content'], data_recv['pg_action'])

        # logline1 = pgdb_admin.pg_process(data_recv['content'], ['sort_debug', object_list_current])
        
        if obs_stag == 'sent':
            continue
    if data_recv['pg_action'][0] == 'insert':
        # 设置 object_list_all 表中 obj_id 的默认值
        pgdb_admin.pg_process(pg_action=['set_default', 'object_list_all'])

        # 将新插入 object_list_all 中符合条件的记录，转入 object_list_current
        pgdb_admin.pg_process(pg_action=['insert_init', object_list_current])

        object_obs_timewindow(location,current_utc_datetime)

    if data_recv['pg_action'][0] == 'truncate':
        pgdb_admin.pg_process(pg_action=data_recv['pg_action'])
        continue

    return_data = object_sort(location,current_utc_datetime)
    # 分配望远镜
    insert_flag = telescope_assign()

    # debug
    '''if data_recv['pg_action'][0] == 'update':
        if obs_stag != logline1:
            logline2 = pgdb_admin.pg_process(data_recv['content'], ['sort_debug', object_list_current])
            write_log(logpath, current_time)
            write_log(logpath, data_recv)
            write_log(logpath, obs_stag)
            write_log(logpath, logline1)
            write_log(logpath, logline2)
            write_log(logpath, '')
'''
    if insert_flag:
        pgdb_admin.pg_process(pg_action=['set_default', 'object_list_all'])
        pgdb_admin.pg_process(pg_action=['insert_init', object_list_current])
        object_obs_timewindow(location,current_utc_datetime)
        return_data = object_sort(location,current_utc_datetime)

    if return_data and return_data[1] not in ['pass', 'break', 'rescheduled', 'complete', 'unobservable']:
        client.Send({'obj_id':return_data[0]})

    t_e = datetime.datetime.now()
    print 'Done. Total:', (t_e-t_b).total_seconds()
