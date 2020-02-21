#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import imp
import time
import datetime
import operator
import threading
from pydash import at
from pd_func_tools import load_params, sql_act, pg_act, check_F30_sync, check_ser_socket_background, get_ser_config, get_cam_config
from communication_client import Client
from ObservationPlanUpload import ObservationPlanUpload
from ToP_obs_plan_insert_DB import insert_to_ba_db,update_to_ba_db,update_pointing_lalalert
from pd_followup import pd_followup
from pd_socket_client import pd_socket_client

###
client = Client('plan_dispatch')
client_og = Client('object_generator')
##
send_objs = []
##
###

def db_init():
    pd_log_tab = 'pd_log_current'
    running_list_cur = 'object_running_list_current'
    date_c = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    ## pd_log_tab
    sql = 'insert into '+pd_log_tab.replace('current','history')+' (obj_id, obj_name, obj_sent_time, obj_dist_time, obj_comp_time, group_id,unit_id,obj_sent_id,obj_dist_id,obs_stag,date_cur,priority,ser_log) SELECT obj_id, obj_name, obj_sent_time, obj_dist_time, obj_comp_time, group_id,unit_id,obj_sent_id,obj_dist_id,obs_stag,date_cur,priority,ser_log FROM '+pd_log_tab+' WHERE date_cur<'+"'"+ date_c +"'"
    sql_act(sql,0)
    sql = 'delete from '+pd_log_tab+' WHERE date_cur<'+"'"+ date_c +"'"
    sql_act(sql,0)
    ## running_list_cur
    sql = 'insert into '+running_list_cur.replace('current','history')+' (cmd,op_sn,op_time,op_type,obj_id,obj_name,observer,objra,objdec,objepoch,objerror,group_id,unit_id,obstype,obs_stra,grid_id,field_id,ra,dec,epoch,imgtype,filter,expdur,delay,frmcnt,priority,begin_time,end_time,run_name,pair_id,note,mode) SELECT cmd,op_sn,op_time,op_type,obj_id,obj_name,observer,objra,objdec,objepoch,objerror,group_id,unit_id,obstype,obs_stra,grid_id,field_id,ra,dec,epoch,imgtype,filter,expdur,delay,frmcnt,priority,begin_time,end_time,run_name,pair_id,note,mode FROM '+running_list_cur+' WHERE op_time<'+"'"+ date_c +"'"
    sql_act(sql,0)
    sql = 'delete from '+running_list_cur+' WHERE op_time<'+"'"+ date_c +"'"
    sql_act(sql,0)

def pd_init():
    global log_file
    cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    if not os.path.exists('obslogs'):
        os.system('mkdir obslogs')
    log_file = open("obslogs/log_%s.txt" % cur_date, "a+")
    while True:
        cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        time_h = time.strftime("%H:%M", time.localtime(time.time()))
        if time_h == '07:50':
            db_init()
            log_file.close()
            log_file = open("obslogs/log_%s.txt" % cur_date, "a+")
            log_file.write("# obj_name objrank begin_time end_time group_id #\n")
            time.sleep(65)

def check_all_device():
    '''
    check service/gtoaes, camagent
    '''
    devs_all = load_params('./pd_params.json')["ssh_socket"]
    devs = devs_all.keys()
    for dev in devs:
        if 'ser' in dev:
            if 'GWAC' in dev:
                dev_type = 'gtoaes'
                cmd = 'ps -ef | grep gtoaes | grep -v grep'
                ser_ip, ser_port = get_ser_config('XL001')[:]
            else:
                dev_type = 'gftservice'
                cmd = 'ps -ef | grep gftservice | grep -v grep'
                if 'w60' in dev:
                    ser_ip, ser_port = get_ser_config('XL002','1')[:]
                elif 'e60' in dev:
                    ser_ip, ser_port = get_ser_config('XL002','2')[:]
                else:
                    ser_ip, ser_port = get_ser_config('XL003')[:]
            res = pd_socket_client(ser_ip, ser_port, cmd)
        else:
            dev_type = 'camagent'
            cmd = 'ps -ef | grep camagent | grep -v grep'
            if 'w60' in dev:
                cam_ip, cam_port = get_cam_config('1')[:]
            elif 'e60' in dev:
                cam_ip, cam_port = get_cam_config('2')[:]
            else:
                ser_ip, ser_port = get_cam_config('3')[:]
            res = pd_socket_client(cam_ip, cam_port, cmd)
        if not res:
            print '\n\033[0;31mWARNING:\033[0m The %s of %s is NOT ONLINE!\n' % (dev_type, devs_all[dev][0])
    time.sleep(30)

def get_obj_infs(obj):
    infs = {}
    sql = "select column_name from information_schema.columns where table_schema='public' and table_name='object_list_all'"
    cols = sql_act(sql)
    sql = "SELECT * FROM object_list_all WHERE obj_id='" + obj + "'"
    infs_list = sql_act(sql)
    if cols and infs_list:
        for i in range(1,len(cols)):
            infs[cols[i][0]] = str(infs_list[0][i])
            # if infs_list[0][i]:
            #     infs[cols[i][0]] = str(infs_list[0][i])
            # else:
            #     infs[cols[i][0]] = infs_list[0][i]
        if infs['filter'] == 'clear':
            if infs['group_id']  == "XL002":
                infs['filter'] = "Lum"
            if infs['group_id']  == "XL003":
                infs['filter'] = "R"
                print "\n\033[0;31mWARNING:\033[0m The filter of %s input Error, using filter R." % obj
                #warn_obs[obj]= "%s \033[0;31mWARNING:\033[0m Filter Error when get_obj_infs, using filter R." % obj
        if len(infs['obj_name']) > 20:
            if infs['group_id'] in ['XL002', 'XL003']:
                #print "\n\033[0;31mWARNING:\033[0m The name of %s is too long, attention please!" % obj
                infs['obj_name'] = infs['obj_name'][:20]
    return infs

def check_sent_insert(obj): ### pd_log_current
    time.sleep(0.5)
    res = pg_act('pd_log_current','select',[['id','obj_sent_time'],{'obj_id':obj,'obs_stag':'sent'},'ORDER BY id DESC LIMIT 1'])
    if res and res[0][0] and res[0][1] == None:
        return res[0][0]
    else:
        return 0

def check_sent_update(id,colu={}): ### pd_log_current
    time.sleep(0.5)
    colu_keys = colu.keys()
    res = pg_act('pd_log_current','select',[[','.join(colu_keys)],{'id':id}])
    if res:
        mk = 0
        for n in range(len(colu_keys)):
            if res[0][n] and res[0][n] == colu[colu_keys[n]]:
                mk = 1
            else:
                mk = 0
        if mk == 1:
            return 1
        else:
            return 0
    else:
        return 0

def check_list_update(obj,sta): ### object_list_current
    time.sleep(0.5)
    res = pg_act('object_list_current','select',[['obs_stag'],{'obj_id':obj}])
    if res and res[0][0] == sta:
        return 1
    else:
        return 0

def Ra_to_h(ra):
    if ':' in ra:
        RaList = map(float,ra.split(':'))
        return str(RaList[0] + RaList[1]/60 + RaList[2]/3600)
    elif abs(float(ra)) <= 360:
        a = float(ra)/15
        return str(a)
    else:
        exit('\n\033[0;31mWARNING:\033[0m Wrong RA.')

def send_obj(obj,obj_infs,group_id,unit_id):
    infs = obj_infs
    ##
    obj_name, observer, obs_type, objra, objdec, objepoch, objerror, imgtype, filter, expdur, delay, frmcnt, priority = at(infs, 'obj_name', 'observer', 'obs_type', 'objra', 'objdec', 'objepoch', 'objerror', 'imgtype', 'filter', 'expdur', 'delay', 'frmcnt', 'priority')
    ##
    date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    send_beg_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    #print send_beg_time
    if group_id == 'XL001':
        ser_ip, ser_port = get_ser_config(group_id)[:]
        if obs_type == 'cal':
            unit_id = infs['unit_id']
            cmd = "tryclient 'take_image group_id=001 unit_id=%s imgtype=%s expdur=%s frmcnt=%s'" % (unit_id, obj_name, expdur, frmcnt)
            pd_socket_client(ser_ip, ser_port, cmd)
            #######
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            #######
            time.sleep(3)
            send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            return [send_beg_time, send_end_time, pd_log_id]
        objsource = infs['objsour']
        op_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        grid_id = 'G0014'
        try:
            strs = obj_name.split('_')
        except:
            field_id = obj_name
        else:
            grid_id = strs[0]
            field_id = strs[1]
        if frmcnt != '-1':
            m1 = float(expdur)
            m2 = float(delay)
            n = int(frmcnt)
            b_time = op_time
            e_time = datetime.datetime.utcfromtimestamp(time.time()+(m1+m2)*n).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            #b_time = "0000-00-00T00:00:00"
            b_time = op_time
            e_time = "0000-00-00T00:00:00"
        cmd = "tryclient 'append_gwac Op_sn=%s, Op_time=%s, Op_type=obs,Group_ID=001, Unit_ID=%s, ObsType=%s, Grid_ID=%s, Field_ID=%s, Obj_ID=%s,RA=%s, DEC=%s, Epoch=2000, ObjRA=%s, ObjDEC=%s, ObjEpoch=%s, ObjError=%s, ImgType=%s,expdur=%s, delay=%s, frmcnt=%s, priority=%s, begin_time=%s, end_time=%s Pair_ID=0'" % (obj,op_time,unit_id,obs_type,grid_id,field_id,objsource,objra,objdec,objra,objdec,objepoch,objerror,imgtype,expdur,delay,frmcnt,priority,b_time,e_time)
        #print cmd
        pd_socket_client(ser_ip, ser_port, cmd)
        #######
        pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
        while True:
            pd_log_id = check_sent_insert(obj)
            if pd_log_id:
                pd_log_id = str(pd_log_id)
                break
        #######
        time.sleep(3)
        send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    if group_id == 'XL002':
        object_ra = Ra_to_h(objra)
        objsource = infs['objsour']
        if unit_id == '001':
            cam_id = '1'
            path = '/home/w60ccd/pd-socket'
        elif unit_id == '002':
            cam_id = '2'
            path = '/home/e60ccd/pd-socket'
        ser_ip, ser_port = get_ser_config(group_id, cam_id)[:]
        if objsource == 'Block':
            cmd = "cd %s/ && nohup bash check_obs_start.sh %s > /dev/null 2>&1" % (path, obj)
            pd_socket_client(ser_ip, ser_port, cmd)
            #####
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent','obj_dist_id':cam_id}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            #####
        if obs_type == 'cal':
            if obj_name == 'flat':
                if cam_id == '1':
                    cmd = "autostop && cd %s && nohup sh boot_flat_f60.sh %s %s %s %s > /dev/null 2>&1" % (path, cam_id, filter, expdur, frmcnt)
                elif cam_id == '2':
                    cmd = "autostart && append_object flat %s %s 2000 3 %s %s %s " % (object_ra, objdec, expdur, frmcnt, filter)
            elif obj_name == 'bias':
                cmd = "autostop && bias %s 2 0 %s" % (cam_id, frmcnt)
            elif obj_name == 'dark':
                cmd = "autostop && dark %s 2 %s %s" % (cam_id, expdur, frmcnt)
            pd_socket_client(ser_ip, ser_port, cmd)
            #####
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent','obj_dist_id':cam_id}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            #####
        else:
            if objsource == 'GWAC_followup':
                run_name = infs['run_name']
                cmd = "append_plan %s %s && append_object %s %s %s %s 4 %s %s %s %s -1 -1 %s && append_plan gwac default " % (observer, obs_type, obj_name, object_ra, objdec, objepoch, expdur, frmcnt, filter, priority, run_name)
            else:
                cmd = "append_plan %s %s && append_object %s %s %s %s 4 %s %s %s %s && append_plan gwac default " % (observer, obs_type, obj_name, object_ra, objdec, objepoch, expdur, frmcnt, filter, priority)
            pd_socket_client(ser_ip, ser_port, cmd)
            #####
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent','obj_dist_id':cam_id}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            #####
        time.sleep(1.5)
        send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    if group_id == 'XL003':
        #unit_id = '001'
        objsource = infs['objsour']
        obs_stra = infs['obs_stra']
        ser_ip, ser_port = get_ser_config(group_id)[:]
        if objsource == 'Block':
            path = '/home/ccduser/pd-socket'
            cmd = "cd %s/ && nohup bash check_obs_start.sh %s > /dev/null 2>&1" % (path, obj) ##cd /home/ccduser/pd-socket && nohup bash check_obs_start.sh 174180 > /dev/null 2>&1
            pd_socket_client(ser_ip, ser_port, cmd)
            time.sleep(1.5)
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        elif obs_type == 'cal':
            cam_id = '3'
            if obj_name == 'flat':
                object_ra = Ra_to_h(str(objra))
                cmd = "autostart && append_object flat %s %s 2000 3 %s %s %s " % (object_ra, objdec, expdur, frmcnt, filter)
            elif obj_name == 'bias':
                cmd = "autostop && bias %s 2 0 %s" % (cam_id, frmcnt)
            elif obj_name == 'dark':
                cmd = "autostop && dark %s 2 %s %s" % (cam_id, expdur, frmcnt)
            pd_socket_client(ser_ip, ser_port, cmd)
            #####
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent','obj_dist_id':cam_id}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            #####
            time.sleep(1.5)
            send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        else:
            if obs_stra == 'dithering':
                ra_shift = infs['ra_shift']
                dec_shift = infs['dec_shift']
                if ra_shift == 'None':###############
                    ra_shift = 0
                if dec_shift == 'None':##############
                    dec_shift = 0
                for i in range(int(frmcnt)):
                    objra = float(objra) + float(ra_shift)
                    objdec = float(objdec) + float(dec_shift)
                    object_ra = Ra_to_h(str(objra))
                    cmd = "append_plan %s %s ; append_object %s %s %s %s 4 %s %s %s %s ; append_plan gwac default " % (observer, obs_type, obj_name, object_ra, str(objdec), objepoch, expdur, '1', filter, priority)
                    pd_socket_client(ser_ip, ser_port, cmd)
                    if i == 0:
                        time.sleep(1.5)
                        send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        time.sleep(1.5)
            else:
                object_ra = Ra_to_h(objra)
                cmd = "append_plan %s %s ; append_object %s %s %s %s 4 %s %s %s %s ; append_plan gwac default " % (observer, obs_type, obj_name, object_ra, objdec, objepoch, expdur, frmcnt, filter, priority)
                pd_socket_client(ser_ip, ser_port, cmd)
                time.sleep(1.5)
                send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            #####
            pg_act('pd_log_current','insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
            while True:
                pd_log_id = check_sent_insert(obj)
                if pd_log_id:
                    pd_log_id = str(pd_log_id)
                    break
            #####
    log_file.write('##\n%s,\n%s,\nsend_beg_time:%s,\nsend_end_time:%s\n##' % (obj, cmd, send_beg_time, send_end_time))
    return [send_beg_time, send_end_time, pd_log_id]

def insert_to_db_in_beg(obj,obj_infs,pd_log_id):
    infs = obj_infs
    ###
    obj_name, objsource, observer, obs_type, obs_stra, objra, objdec, objepoch, objerror, imgtype, filter, expdur, delay, frmcnt, priority, run_name = at(infs, 'obj_name', 'objsour', 'observer', 'obs_type', 'obs_stra', 'objra', 'objdec', 'objepoch', 'objerror', 'imgtype', 'filter', 'expdur', 'delay', 'frmcnt', 'priority', 'run_name')
    ###
    res = pg_act('pd_log_current','select',[['group_id','unit_id','obj_sent_time'],{'id':pd_log_id}])
    if res:
        group_id,unit_id,obj_sent_time = res[0][:]
        if not obj_sent_time:
            obj_sent_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        b_time = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(obj_sent_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        return
    op_time = b_time
    ###
    if group_id == 'XL001':
        grid_id = 'G0014'
        if 'G' in obj_name:
            try:
                strs = obj_name.split('_')
            except:
                grid_id = 0
                field_id = obj_name
                #warn_obs['obs'] = 'The obj_name of %s is Wrong.' % obj
                print 'The obj_name of %s is Wrong.' % obj
            else:
                grid_id = strs[0]
                field_id = strs[1]
        else:
            grid_id = 0
            field_id = obj_name
            #warn_obs['obs'] = 'The obj_name of %s is Wrong.' % obj
            print 'The obj_name of %s is Wrong.' % obj
        if frmcnt != '-1':
            m1 = float(expdur)
            m2 = float(delay)
            n = int(frmcnt)
            b_time = op_time
            e_time = datetime.datetime.utcfromtimestamp(time.time()+(m1+m2)*n).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            #b_time = "0000-00-00T00:00:00"
            b_time = op_time
            e_time = "0000-00-00T00:00:00"
        ###
        uploadUrl = 'http://172.28.8.8/gwebend/observationPlanUpload.action'
        opTime = op_time.replace('T',' ')
        beginTime = b_time.replace('T',' ')
        endTime = e_time.replace('T',' ')
        opSn,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,pairId = [obj,'obs','001',unit_id,obs_type,grid_id,field_id,objsource,objra,objdec,objepoch,objra,objdec,objepoch,objerror,imgtype,str(int(float(expdur))),str(int(float(delay))),frmcnt,priority,'0'][:]
        tplan = ObservationPlanUpload(uploadUrl, opSn,opTime,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,beginTime,endTime,pairId)
        tplan.sendPlan()
        ###
        cmd_n = 'append_gwac'
        pg_act('object_running_list_current','insert',[{'cmd':cmd_n,'op_sn':obj,'op_time':op_time,'op_type':'obs','obj_id':obj,'obj_name':obj_name,'observer':observer,\
            'objra':objra,'objdec':objdec,'objepoch':objepoch,'objerror':objerror,'group_id':group_id,'unit_id':unit_id,'obstype':obs_type,\
                'obs_stra':obs_stra,'grid_id':grid_id,'field_id':field_id,'ra':objra,'dec':objdec,'imgtype':imgtype,'filter':filter,'expdur':expdur,'delay':delay,\
                    'frmcnt':frmcnt,'priority':priority,'begin_time':b_time,'end_time':e_time,'run_name':run_name,'pair_id':'0','mode':'observation','note':''}])
        ###
        try:
            objsour_word = objsource.split('_')
        except:
            pass
        else:
            if objsour_word[0] == 'GW':
                trigger = objsour_word[2]
                insert_to_ba_db(objsource,obj,group_id,unit_id,filter,grid_id,field_id,objra,objdec,'tiling',beginTime,endTime,expdur,'observation','scheduled')
                ###
                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_name + "'" + 'and objsour=' + "'" + objsource + "'"
                res = sql_act(sql)
                if res:
                    objrank = res[0][0]
                else:
                    objrank = '0'
                update_pointing_lalalert(trigger,'GWAC',grid_id,field_id,objrank,'scheduled')
                ###
        ###
    if group_id in ['XL002','XL003']:
        ##
        #unit_id = '001'
        e_time = "0000-00-00T00:00:00"
        ##
        cmd_n = 'append_object'
        pg_act('object_running_list_current','insert',[{'cmd':cmd_n,'op_sn':obj,'op_time':op_time,'op_type':'obs','obj_id':obj,'obj_name':obj_name,'observer':observer,\
            'objra':objra,'objdec':objdec,'objepoch':objepoch,'objerror':objerror,'group_id':group_id,'unit_id':unit_id,'obstype':obs_type,\
                'obs_stra':obs_stra,'ra':objra,'dec':objdec,'imgtype':imgtype,'filter':filter,'expdur':expdur,'delay':delay,\
                    'frmcnt':frmcnt,'priority':priority,'begin_time':b_time,'end_time':e_time,'run_name':run_name,'pair_id':'0','mode':'observation','note':''}])
        ###
        try:
            objsour_word = objsource.split('_')
            trigger_type = objsour_word[0]
            version = objsour_word[1]
            trigger = objsour_word[2]
        except:
            pass
        else:
            if trigger_type == 'GW':
                if 'G0' in obj_name:
                    try:
                        strs = obj_name.split('_')
                    except:
                        grid_id = 'G0000'
                        field_id = obj_name
                    else:
                        grid_id = strs[0]
                        field_id = strs[1]
                else:
                    grid_id = 'G0000'
                    field_id = obj_name
                beginTime = b_time.replace('T',' ')
                endTime = e_time.replace('T',' ')
                if group_id == 'XL002':
                    up_stra = 'pointing'
                else:
                    up_stra = 'tiling'
                insert_to_ba_db(objsource,obj,group_id,unit_id,filter,grid_id,field_id,objra,objdec,up_stra,beginTime,endTime,expdur,'observation','scheduled')
                ###
                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_name + "'" + 'and objsour=' + "'" + objsource + "'"
                res = sql_act(sql)
                if res:
                    objrank = res[0][0]
                else:
                    objrank = '0'
                if group_id == 'XL002':
                    name_telescope = 'F60'
                elif group_id == 'XL003':
                    name_telescope = 'F30'
                update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,'scheduled')
                ###
                sql = 'select "Op_Obj_ID" from trigger_obj_field_op_sn where "Trigger_ID"=' + "'" + trigger + "'" + ' and "Serial_num"=' + "'" + version + "'" + ' and "Obj_ID"=' + "'" + obj_name + "'"
                res = sql_act(sql)
                if res:
                    objs = []
                    for i in res:
                        objs.append(i[0])
                    objs = list(set(objs))
                    if len(objs) == 1:
                        try:
                            obj_nms = objs[0].split('|')
                        except:
                            obj_nms = objs
                        for obj_nm in obj_nms:
                            if 'G0' in obj_nm:
                                try:
                                    strs = obj_nm.split('_')
                                except:
                                    grid_id = 'G0000'
                                    field_id = obj_nm
                                else:
                                    grid_id = strs[0]
                                    field_id = strs[1]
                            else:
                                grid_id = 'G0000'
                                field_id = obj_nm
                            ###
                            sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_nm + "'" + 'and objsour=' + "'" + objsource + "'"
                            res = sql_act(sql)
                            if res:
                                objrank = res[0][0]
                            else:
                                objrank = '0'
                            if group_id == 'XL002':
                                name_telescope = 'F60'
                            elif group_id == 'XL003':
                                name_telescope = 'F30'
                            else:
                                pass
                            update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,'scheduled')
                            ###
                    else:
                        print '\nWrong: Got too many res when send_db_in_beg'

def update_to_db_in_end(obj,obj_infs,pd_log_id):
    infs = obj_infs
    res = pg_act('pd_log_current','select',[['group_id','unit_id','obj_dist_id','obj_comp_time','obs_stag'],{'id':pd_log_id}])
    if res:
        group_id, unit_id, log_dist_id, obj_comp_time,obs_stag = res[0][:]
        if group_id in ['XL002','XL003']:
            if log_dist_id == '1':
                unit_id = '001'
            if log_dist_id == '2':
                unit_id = '002'
            if log_dist_id == '3':
                unit_id = '001'
    else:
        return
    ##
    if not obj_comp_time:
        obj_comp_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
    if obs_stag == 'complete':
        up_stag = obs_stag
    elif obs_stag == 'break':
        up_stag = 'broken'
    elif obs_stag == 'pass':
        up_stag = 'pass obs time window'
    elif obs_stag == 'sent':
        pg_act('pd_log_current','update', [{'obj_comp_time':obj_comp_time,'obs_stag':'break'},{'id':pd_log_id}])
        obs_stag = 'break'
        up_stag = 'broken'
    else:
        obs_stag = 'break'
        up_stag = 'broken'
    ##
    e_time = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(obj_comp_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
    endTime = e_time.replace('T',' ')
    objsource = infs['objsour']
    obj_name = infs['obj_name']
    #####
    pg_act('object_running_list_current','update', [{'end_time':e_time,'unit_id':unit_id},{'obj_id':obj}])
    #####
    if group_id == 'XL001':
        #####
        update_to_ba_db(objsource, obj, up_stag, endTime)
        ###
        try:
            objsour_word = objsource.split('_')
            trigger_type = objsour_word[0]
            trigger = objsour_word[2]
        except:
            pass
        else:
            if trigger_type == 'GW':
                grid_id = 'G0014'
                if 'G0' in obj_name:
                    try:
                        strs = obj_name.split('_')
                    except:
                        field_id = obj_name
                        print 'The obj_name of %s is Wrong.' % obj
                    else:
                        grid_id = strs[0]
                        field_id = strs[1]
                else:
                    #grid_id = 0
                    field_id = obj_name
                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_name + "'" + 'and objsour=' + "'" + objsource + "'"
                res = sql_act(sql)
                if res:
                    objrank = res[0][0]
                else:
                    objrank = '0'
                update_pointing_lalalert(trigger,'GWAC',grid_id,field_id,objrank,up_stag)
        #####
    if group_id in ['XL002','XL003']:
        try:
            objsour_word = objsource.split('_')
            trigger_type = objsour_word[0]
            version = objsour_word[1]
            trigger = objsour_word[2]
        except:
            pass
        else:
            if trigger_type == 'GW':
                if 'G0' in obj_name:
                    try:
                        strs = obj_name.split('_')
                    except:
                        grid_id = 'G0000'
                        field_id = obj_name
                    else:
                        grid_id = strs[0]
                        field_id = strs[1]
                else:
                    grid_id = 'G0000'
                    field_id = obj_name
                #####
                update_to_ba_db(objsource, obj, up_stag, endTime)
                #####
                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_name + "'" + 'and objsour=' + "'" + objsource + "'"
                res = sql_act(sql)
                if res:
                    objrank = res[0][0]
                else:
                    objrank = '0'
                if group_id == 'XL002':
                    name_telescope = 'F60'
                elif group_id == 'XL003':
                    name_telescope = 'F30'
                else:
                    pass
                update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,up_stag)
                ###
                sql = 'select "Op_Obj_ID" from trigger_obj_field_op_sn where "Trigger_ID"=' + "'" + trigger + "'" + ' and "Serial_num"=' + "'" + version + "'" + ' and "Obj_ID"=' + "'" + obj_name + "'"
                res = sql_act(sql)
                if res:
                    objs = []
                    for i in res:
                        objs.append(i[0])
                    objs = list(set(objs))
                    if len(objs) == 1:
                        try:
                            obj_nms = objs[0].split('|')
                        except:
                            obj_nms = objs
                        for obj_nm in obj_nms:
                            if 'G0' in obj_nm:
                                try:
                                    strs = obj_nm.split('_')
                                except:
                                    grid_id = 'G0000'
                                    field_id = obj_nm
                                else:
                                    grid_id = strs[0]
                                    field_id = strs[1]
                            else:
                                grid_id = 'G0000'
                                field_id = obj_nm
                            ###
                            sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_nm + "'" + 'and objsour=' + "'" + objsource + "'"
                            res = sql_act(sql)
                            if res:
                                objrank = res[0][0]
                            else:
                                objrank = '0'
                            if group_id == 'XL002':
                                name_telescope = 'F60'
                            elif group_id == 'XL003':
                                name_telescope = 'F30'
                            else:
                                pass
                            update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,up_stag)
                            ###
                    else:
                        print '\nWrong: Got too many res when send_db_in_end'
    log_file.write('#\n%s The obj of %s is complete.#' % (obj_comp_time, obj))

def check_log_sent(obj, send_beg_time, send_end_time, pd_log_id): ### after check_sent_insert
    res = pg_act('pd_log_current','select',[['obj_name','group_id','unit_id'],{'id':pd_log_id}])
    if res:
        obj_name, group_id, unit_id = res[0][:]
    else:
        #warn_obs[obj] = '%s \033[0;31mWARNING:\033[0m WRONG in check_log_sent !' % obj
        print '%s \033[0;31mWARNING:\033[0m WRONG in check_log_sent !' % obj
        return ''
    if group_id == 'XL001':
        time.sleep(1.5)
        ser_ip, ser_port = get_ser_config(group_id)[:]
        cmd = 'ls /var/log/gtoaes/gtoaes*.log | sort -r'
        logs = pd_socket_client(ser_ip, ser_port, cmd)
        if logs:
            sent_mark = 0
            date_mark = 0
            for log in logs:
                date_mark += 1
                if date_mark == 3:
                    break
                log = log.strip()
                log_date = re.search(r"20\d\d\d\d\d\d", log).group(0)
                log_date = time.strftime("%Y-%m-%d",time.strptime(log_date, "%Y%m%d"))
                #print log_date
                if obj_name in ['flat','bias','dark']:
                    cmd = "tac %s | grep -a '.*take_image <name=%s.*on <001:%s:all>'" % (log, obj_name, unit_id)
                    res = pd_socket_client(ser_ip, ser_port, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            print "\n######"+item
                            log_sent_time = re.search(r"\d\d:\d\d:\d\d", item).group(0)
                            log_sent_time = "%s %s" % (log_date, log_sent_time)
                            if send_beg_time <= log_sent_time <= send_end_time:
                                log_sent_id = re.search(r'goes running on <001:(.*?)>', item).group(1)
                                sent_mark = 1
                                break
                            else:
                                sent_mark = 0
                        if sent_mark == 1:
                            break
                else:
                    cmd = "tac %s | grep -a '.*plan<%s> goes running on .*'" % (log, obj)
                    res = pd_socket_client(ser_ip, ser_port, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            print "\n######"+item
                            log_sent_time = re.search(r"\d\d:\d\d:\d\d", item).group(0)
                            log_sent_time = "%s %s" % (log_date, log_sent_time)
                            if send_beg_time <= log_sent_time <= send_end_time:
                                log_sent_id = re.search(r'goes running on <001:(.*?)>', item).group(1)
                                sent_mark = 1
                                break
                            else:
                                sent_mark = 0
                        if sent_mark == 1:
                            break
            if sent_mark == 1:
                #######
                pg_act('pd_log_current','update', [{'ser_log':log,'obj_sent_time':log_sent_time,'obj_sent_id':log_sent_id},{'id':pd_log_id}])
                while True:
                    if check_sent_update(pd_log_id,{'ser_log':log,'obj_sent_time':log_sent_time,'obj_sent_id':log_sent_id}):
                        break
                #######
                return 1
            else:
                #warn_obs[obj] = "\n\033[0;31mWARNING:\033[0m There is no sent inf of %s in gtoaes !" % obj
                return 0
        else:
            #warn_obs[obj] = "%s \033[0;31mWARNING:\033[0m There is no gtoaes log of %s when check_sent !" % (obj, group_id)
            print "\n%s \033[0;31mWARNING:\033[0m There is no gtoaes log of %s when check_sent !" % (obj, group_id)
            return 0
    if group_id in ['XL002','XL003']:
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        if group_id == 'XL002':
            if unit_id == '001':
                cam_id = '1'
            else:
                cam_id = '2'
        else:
            cam_id = '3'
        ser_ip, ser_port = get_ser_config(group_id, cam_id)[:]
        cmd = 'ls /tmp/gftservice*.log | sort -r'
        logs = pd_socket_client(ser_ip, ser_port, cmd)
        if logs:
            sent_mark = 0
            for log in logs:
                log = log.strip()
                cmd = "tac " + log + " | grep 'append object<id.*>.*" + obj_name + "'"
                res = pd_socket_client(ser_ip, ser_port, cmd)
                if res:
                    for item in res:
                        item = item.strip()
                        print "\n######"+item
                        log_sent_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                        if send_beg_time <= log_sent_time <= send_end_time:
                            log_sent_id = re.search(r'<id = (.*?)>', item).group(1)
                            sent_mark = 1
                            break
                        else:
                            sent_mark = 0
                    if sent_mark == 1:
                        break
                if sent_mark == 0:
                    cmd = "tail " + log + ' | grep "20[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*" | sort -r'
                    res = pd_socket_client(ser_ip, ser_port, cmd)
                    if res:
                        res = res[0].strip()
                        re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res)
                        if re_time and re_time.group(0) <= time_limit:
                            break
            if sent_mark == 1:
                #######
                pg_act('pd_log_current','update', [{'ser_log':log,'obj_sent_time':log_sent_time,'obj_sent_id':log_sent_id},{'id':pd_log_id}])
                while True:
                    if check_sent_update(pd_log_id,{'ser_log':log,'obj_sent_time':log_sent_time,'obj_sent_id':log_sent_id}):
                        break
                #######
                return 1
            else:
                #warn_obs[obj] = "\n\033[0;31mWARNING:\033[0m There is no sent inf of %s in gftservice !" % obj
                return 0
        else:
            print "\n%s \033[0;31mWARNING:\033[0m There is no gftservice log of %s when check_sent !" % (obj, group_id)
            return 0

def check_log_dist(obj, pd_log_id):
    res = pg_act('pd_log_current','select',[['group_id','unit_id','ser_log','obj_sent_id','obj_sent_time'],{'id':pd_log_id}])
    if res:
        group_id, unit_id, ser_log, log_sent_id, log_sent_time = res[0][:]
    else:
        print '%s \033[0;31mWARNING:\033[0m WRONG in check_log_dist !' % obj
        return 'Wrong'
    if group_id == 'XL001':
        log_dist_id, log_dist_time = [log_sent_id, log_sent_time][:]
        pg_act('pd_log_current','update',[{'obj_dist_time':log_dist_time,'obj_dist_id':log_dist_id},{'id':pd_log_id}])
        while True:
            if check_sent_update(pd_log_id,{'obj_dist_time':log_dist_time,'obj_dist_id':log_dist_id}):
                break
        return 1
    if group_id == 'XL002':
        if unit_id == '001':
            cam_id = '1'
        else:
            cam_id = '2'
        ser_ip, ser_port = get_ser_config(group_id, cam_id)[:]
        dist_mark = 0
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        ###
        if ser_log:
            log = ser_log
            cmd = "tac " + log + " | grep 'get observation plan <id = " + log_sent_id + ">'"
            res = pd_socket_client(ser_ip, ser_port, cmd)
            if res:
                for item in res:
                    item = item.strip()
                    #print "\n"+item
                    log_dist_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                    if log_dist_time >= log_sent_time:
                        log_dist_id = re.search(r'<system id = (.*?)>', item).group(1)
                        dist_mark = 1
                        break
                    else:
                        dist_mark = 0 
            ###          
            if dist_mark == 0:
                cmd = 'ls /tmp/gftservice*.log | sort -r'
                logs = pd_socket_client(ser_ip, ser_port, cmd)
                if logs:
                    #logs.reverse()
                    for log in logs:
                        log = log.strip()
                        if log > ser_log:
                            cmd = "tac " + log + " | grep 'ERROR: daemon has been in running'"
                            res = pd_socket_client(ser_ip, ser_port, cmd)
                            if not res:
                                dist_mark = 2
                                print "\n%s \033[0;31mWARNING:\033[0m Break it for rebooting the gftservice !" % obj
                                break
                        else:
                            break
                    # log = logs[0].strip()
                    # if log > ser_log:
                    #     #dist_mark = 2
                    #     warn_obs[obj] = "%s \033[0;31mWARNING:\033[0m Please check the gtfservice log, then break it or not !" % obj
                else:
                    print "\n%s \033[0;31mWARNING:\033[0m There is no gftservice log of %s when check_dist !" % (obj, group_id)
        else:
            cmd = 'ls /tmp/gftservice*.log | sort -r'
            logs = pd_socket_client(ser_ip, ser_port, cmd)
            if logs:
                for log in logs:
                    log = log.strip()###
                    cmd = "tac " + log + " | grep 'get observation plan <id = " + log_sent_id + ">'"
                    res = pd_socket_client(ser_ip, ser_port, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            #print "\n"+item
                            log_dist_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                            if log_dist_time >= log_sent_time:
                                log_dist_id = re.search(r'<system id = (.*?)>', item).group(1)
                                dist_mark = 1
                                break
                            else:
                                dist_mark = 0
                        if dist_mark == 1:
                            break
                    if dist_mark == 0:
                        cmd = "tail " + log + ' | grep "20[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*" | sort -r'
                        res = pd_socket_client(ser_ip, ser_port, cmd)
                        if res:
                            res = res[0].strip()
                            #print res,time_limit
                            re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res)
                            if re_time and re_time.group(0) <= time_limit:
                                break
        ###
        if dist_mark == 2:
            return 2
        elif dist_mark == 1:
            #######
            pg_act('pd_log_current','update',[{'obj_dist_time':log_dist_time,'obj_dist_id':log_dist_id},{'id':pd_log_id}])
            while True:
                if check_sent_update(pd_log_id,{'obj_dist_time':log_dist_time,'obj_dist_id':log_dist_id}):
                    break
            ######
            return 1
        else:
            return 0
    if group_id == 'XL003':
        ser_ip, ser_port = get_ser_config(group_id)[:]
        dist_mark = 0
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        ###
        #print ser_log
        if ser_log:
            log = ser_log.strip()
            cmd = "cat %s | grep -v '^<system 3>'" % log
            res = pd_socket_client(ser_ip, ser_port, cmd)
            if res:
                #print len(res)
                ii = 0
                mark_i = 0
                for item in res:
                    ii += 1
                    item = item.strip()
                    re_time = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?append object<id = %s>" % log_sent_id, item)
                    if mark_i == 0 and re_time and re_time.group(1) == log_sent_time:
                        #print item
                        #print ii
                        mark_i = ii
                    re_null = re.search(r"take NULL object", item)
                    if re_null:
                        if mark_i != 0 and ii > mark_i:
                            #print ii
                            print '%s \033[0;31mWARNING:\033[0m It occured "take NULL object", break it !' % obj
                            dist_mark = 2
                            break
                    res_re = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?<system id = (.*?)>.*?get observation plan <id = %s>" % log_sent_id, item)
                    if res_re:
                        it = res_re.group(0)
                        #print '\n' + it
                        log_dist_time = res_re.group(1)
                        if log_dist_time > log_sent_time or log_dist_time == log_sent_time:
                            log_dist_id = res_re.group(2)
                            dist_mark = 1
                            break
                        else:
                            dist_mark = 0
            ###
            if dist_mark == 0:
                cmd = 'ls /tmp/gftservice*.log | sort -r'
                logs = pd_socket_client(ser_ip, ser_port, cmd)
                if logs:
                    for log in logs:
                        log = log.strip()
                        if log > ser_log:
                            cmd = "tac " + log + " | grep 'ERROR: daemon has been in running'"
                            res = pd_socket_client(ser_ip, ser_port, cmd)
                            if not res:
                                dist_mark = 2
                                print "\n%s \033[0;31mWARNING:\033[0m Break it for rebooting the gftservice !" % obj
                                break
                        else:
                            break
                else:
                    print "\n%s \033[0;31mWARNING:\033[0m There is no gftservice log of %s when check_dist !" % (obj, group_id)
        else:
            cmd = 'ls /tmp/gftservice*.log | sort -r'
            logs = pd_socket_client(ser_ip, ser_port, cmd)
            if logs:
                for log in logs:
                    log = log.strip()
                    cmd = "tail " + log + ' | grep "20[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*" | sort -r'
                    res = pd_socket_client(ser_ip, ser_port, cmd)
                    if res:
                        res = res[0].strip()
                        re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res)
                        if re_time and re_time.group(0) <= time_limit:
                            break
                    cmd = "cat %s | grep -v '^<system 3>'" % log
                    res = pd_socket_client(ser_ip, ser_port, cmd)
                    if res:
                        ii = 0
                        mark_i = 0
                        for item in res:
                            ii += 1
                            item = item.strip()
                            re_time = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?append object<id = %s>" % log_sent_id, item)
                            if mark_i == 0 and re_time and re_time.group(1) == log_sent_time:
                                mark_i = ii
                            re_null = re.search(r"take NULL object", item)
                            if re_null:
                                if mark_i != 0 and ii > mark_i:
                                    print'%s \033[0;31mWARNING:\033[0m It occured "take NULL object", break it !' % obj
                                    dist_mark = 2
                                    break
                            res_re = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?<system id = (.*?)>.*?get observation plan <id = %s>" % log_sent_id, item)
                            if res_re:
                                it = res_re.group(0)
                                #print '\n' + it
                                log_dist_time = res_re.group(1)
                                if log_dist_time >= log_sent_time:
                                    log_dist_id = res_re.group(2)
                                    dist_mark = 1
                                    break
                                else:
                                    dist_mark = 0
                        if dist_mark in [1, 2]:
                            break
        ###
        if dist_mark == 2:
            return 2
        elif dist_mark == 1:
            #######
            pg_act('pd_log_current','update',[{'obj_dist_time':log_dist_time,'obj_dist_id':log_dist_id},{'id':pd_log_id}])
            while True:
                if check_sent_update(pd_log_id,{'obj_dist_time':log_dist_time,'obj_dist_id':log_dist_id}):
                    break
            #######
            return 1
        else:
            return 0

def check_cam_log(obj,obj_infs,pd_log_id):
    proc = 0
    res = pg_act('pd_log_current','select',[['group_id','unit_id','obj_sent_time','obj_dist_id','obj_dist_time','ser_log'],{'id':pd_log_id}])
    if res:
        group_id, unit_id, log_sent_time, log_dist_id, log_dist_time, ser_log = res[0][:]
    else:
        print '%s \033[0;31mWARNING:\033[0m WRONG in check_cam_log !' % obj
        return 'Wrong'
    if group_id == 'XL001':
        ser_ip, ser_port = get_ser_config(group_id)[:]
        cmd = 'ls /var/log/gtoaes/gtoaes*.log | sort -r'
        logs = pd_socket_client(ser_ip, ser_port, cmd)
        if logs:
            com_mark = 0
            date_mark = 0
            for log in logs:
                date_mark += 1
                if date_mark == 3:
                    break
                log = log.strip()
                log_date = re.search(r"20\d\d\d\d\d\d", log).group(0)
                log_date = time.strftime("%Y-%m-%d",time.strptime(log_date, "%Y%m%d"))
                #print log_date
                cmd = "tac %s | grep -a '.*plan<%s> on .* is over'" % (log, obj)
                res = pd_socket_client(ser_ip, ser_port, cmd)
                if res:
                    for item in res:
                        item = item.strip()
                        log_com_time = re.search(r"\d\d:\d\d:\d\d", item).group(0)
                        log_com_time = "%s %s" % (log_date, log_com_time)
                        if log_com_time > log_dist_time:
                            #print "\n"+item
                            com_mark = 1
                            break
                        else:
                            com_mark = 0
                    if com_mark == 1:
                        break
                cmd = "tac %s | grep -a '.*plan<%s> on .* is interrupted for position error'" % (log, obj)
                res = pd_socket_client(ser_ip, ser_port, cmd)
                if res:
                    for item in res:
                        item = item.strip()
                        log_com_time = re.search(r"\d\d:\d\d:\d\d", item).group(0)
                        log_com_time = "%s %s" % (log_date, log_com_time)
                        #print log_com_time
                        if log_com_time > log_dist_time:
                            #print "\n"+item
                            com_mark = 2
                            break
                        else:
                            com_mark = 0
                    if com_mark == 2:
                        break
            if com_mark == 1:
                expdur, delay, frmcnt = at(obj_infs, 'expdur', 'delay', 'frmcnt')
                n = int(frmcnt)
                if n != -1:
                    m1 = float(expdur)
                    m2 = float(delay)
                    end_time_limit = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.mktime(time.strptime(log_dist_time, "%Y-%m-%d %H:%M:%S")) + (m1+m2)*n - 600 )) ### -10 mins
                    if log_com_time < end_time_limit:
                        return [2, log_com_time]
                    else:
                        return [1, log_com_time]
                # else:
                #     return 1
            elif com_mark == 2:
                #######
                print "\n%s \033[0;31mWARNING:\033[0m The obj is broken for position error !" % obj
                return [2, log_com_time]
            else:
                #warn_obs[obj] = "\nThere is no complete inf of %s in gtoaes !" % obj
                beg_t = time.mktime(time.strptime(log_dist_time, "%Y-%m-%d %H:%M:%S"))
                expdur, delay, frmcnt = at(obj_infs, 'expdur', 'delay', 'frmcnt')
                n = int(frmcnt)
                if n != -1:
                    m1 = float(expdur)
                    m2 = float(delay)
                    now_t = time.time()
                    proc = '%.0f%%' % (((now_t-beg_t)/((m1+m2)*n+360))*100)
                return [0, proc]
        else:
            print "\n%s \033[0;31mWARNING:\033[0m There is no gtoaes log of %s when check_com !"% (obj, group_id)
            return 0
    if group_id in ['XL002','XL003']:
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        obj_name, filter, frmcnt = at(obj_infs, 'obj_name','filter', 'frmcnt')
        date_now = datetime.datetime.utcnow().strftime("%y%m%d")
        count = 0
        com_mark = 0
        #####
        # if ser_log:
        #     if group_id == 'XL002':
        #         if unit_id == '001':
        #             cam_id = '1'
        #         else:
        #             cam_id = '2'
        #     else:
        #         cam_id = '3'
        #     ser_ip, ser_port = get_ser_config(group_id, cam_id)[:]
        #     cmd = 'ls /tmp/gftservice*.log | sort -r'
        #     logs = pd_socket_client(ser_ip, ser_port, cmd)
        #     if logs:
        #         for log in logs:
        #             log = log.strip()
        #             if log > ser_log:
        #                 cmd = "tac " + log + " | grep 'ERROR: daemon has been in running'"
        #                 res = pd_socket_client(ser_ip, ser_port, cmd)
        #                 if not res:
        #                     com_mark = 2
        #                     log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        #                     print "\n%s \033[0;31mWARNING:\033[0m Break it for rebooting the gftservice !" % obj
        #                     break
        #             else:
        #                 break
        #     else:
        #         print "\n%s \033[0;31mWARNING:\033[0m There is no gftservice log of %s when check_dist !" % (obj, group_id)
        if com_mark == 0:
            cmd = 'ls /tmp/camagent*.log | sort -r'
            cam_ip, cam_port = get_cam_config(log_dist_id)[:]
            logs = pd_socket_client(cam_ip, cam_port, cmd)
            if logs:
                for log in logs:
                    log = log.strip()
                    cmd = "cat " + log + " | grep 'Image is saved as " + obj_name + ".*_" + filter + "_" + date_now+ "_.*.fit'"
                    res = pd_socket_client(cam_ip, cam_port, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            item_com_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                            if item_com_time > log_sent_time:
                                #print '\n'+item
                                count += 1
                            if count == int(frmcnt):
                                log_com_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                com_mark = 1
                                break
                        if com_mark == 1:
                            break
                    if com_mark == 0:
                        cmd = "tail " + log + ' | grep "20[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*" | sort -r'
                        res = pd_socket_client(cam_ip, cam_port, cmd)
                        if res:
                            res = res[0].strip()
                            re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res)
                            if re_time and re_time.group(0) <= time_limit:
                                break
                        cmd = "tac " + log + " | grep 'ERROR: Filter input error'"
                        res = pd_socket_client(cam_ip, cam_port, cmd)
                        if res:
                            for item in res:
                                item = item.strip()
                                item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                if item_time >= log_dist_time:
                                    print '\n' + item
                                    log_com_time = item_time
                                    com_mark = 2
                                    break
                            if com_mark == 2:
                                break
                        cmd = "tac " + log + " | grep 'New object(Name, RA, DEC):  %s.*'" % obj_name
                        res = pd_socket_client(cam_ip, cam_port, cmd)
                        if res:
                            n_it = res[0].strip()
                            n_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", n_it).group(0)
                            if n_time >= log_dist_time:
                                pass
                            else:
                                cmd = "tac " + log + " | grep 'WARN: Trying to change.*'"
                                res = pd_socket_client(cam_ip, cam_port, cmd)
                                if res:
                                    for item in res:
                                        item = item.strip()
                                        item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                        if item_time >= log_dist_time:
                                            print '\n' + item
                                            log_com_time = item_time
                                            com_mark = 2
                                            break
                                    if com_mark == 2:
                                        break
                        else:
                            cmd = "tac " + log + " | grep 'WARN: Trying to change.*'"
                            res = pd_socket_client(cam_ip, cam_port, cmd)
                            if res:
                                for item in res:
                                    item = item.strip()
                                    item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                    if item_time >= log_dist_time:
                                        print '\n' + item
                                        log_com_time = item_time
                                        com_mark = 2
                                        break
                                if com_mark == 2:
                                    break
            else:
                print "\n%s \033[0;31mWARNING:\033[0m There is no camagent log of %s when check_com !" % (obj, log_dist_id)
                return 0
        if com_mark == 2:
            # print "\n%s \033[0;31mWARNING:\033[0m Filter Error. Please check the system !" % obj
            # return 0
            return [2, log_com_time]
        elif com_mark == 1:
            return [1, log_com_time]
        else:
            #warn_obs[obj] = "\nThere is no obs complete inf about %s in camagent log of %s for now!" % (obj_name, log_dist_id)
            proc = '%.0f%%' % ((count/float(frmcnt))*100)
            return [0, proc]

def check_time_window(obj):
    while True:
        sql = "SELECT tw_begin, tw_end FROM object_list_current WHERE obj_id='" + obj + "'"
        tws = sql_act(sql)
        if tws:
            begin_time, end_time = tws[0][:]
            break
    time_now = datetime.datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S")
    if time_now < end_time:
        if time_now > begin_time:
            return 1
        else:
            return -1
    else:
        return 0

def check_obj_stat_with_cal(obj,obj_infs,pd_log_id,group_id,unit_id,obj_sent_time):
    obj_name, filter, frmcnt = at(obj_infs, 'obj_name', 'filter', 'frmcnt')
    if group_id == 'XL001':
        ser_ip, ser_port = get_ser_config(group_id)[:]
        cmd = 'ls /var/log/gtoaes/gtoaes*.log | sort -r'
        logs = pd_socket_client(ser_ip, ser_port, cmd)
        if logs:
            com_mark = 0
            date_mark = 0
            for log in logs:
                date_mark += 1
                if date_mark == 3:
                    break
                log = log.strip()
                log_date = re.search(r"20\d\d\d\d\d\d", log).group(0)
                log_date = time.strftime("%Y-%m-%d",time.strptime(log_date, "%Y%m%d"))
                cmd = "tac %s | grep -a '.*plan<2147483647> on <001:%s> is over'" % (log, unit_id)
                res = pd_socket_client(ser_ip, ser_port, cmd)
                if res:
                    for item in res:
                        item = item.strip()
                        print "\n######"+item
                        log_time = re.search(r"\d\d:\d\d:\d\d", item).group(0)
                        log_time = "%s %s" % (log_date, log_time)
                        if log_time >= obj_sent_time:
                            log_sent_id = re.search(r'goes running on <001:(.*?)>', item).group(1)
                            com_mark = 1
                            break
                        else:
                            com_mark = 0
                    if com_mark == 1:
                        break
            # if com_mark == 1:
            #     return 1
            # else:
            #     return 0
            if com_mark == 1:
                print "\nThe obj has been observed completely !\n"
                pg_act('pd_log_current','update',[{'obj_comp_time':log_time,'obs_stag':'complete'},{'id':pd_log_id}])
                update_to_db_in_end(obj,obj_infs,pd_log_id)
                client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
    if group_id in ["XL002","XL003"]:
        if group_id == 'XL002':
            if unit_id == '001':
                user = 'w60ccd'
                cam_id = '1'
            elif unit_id == '002':
                user = 'e60ccd'
                cam_id = '2'
        elif group_id == 'XL003':
            user = 'ccduser'
            cam_id = '3'
        cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        cur_year = cur_date[:4]
        cam_ip, cam_port = get_cam_config(cam_id)[:]
        dir = "/home/%s/data/Y%s/%s/" % (user, cur_year, cur_date)
        cmd = "ls -l" + dir + obj_name + "/" + obj_name + "*_"+ filter + "_*_*" + ".fit"
        res = pd_socket_client(cam_ip, cam_port, cmd)
        # if res and len(res) == int(frmcnt):
        #     return 1
        # else:
        #     return 0
        if res and len(res) == int(frmcnt):
            print "\nThe obj has been observed completely !\n"
            pg_act('pd_log_current','update',[{'obj_comp_time':log_time,'obs_stag':'complete'},{'id':pd_log_id}])
            update_to_db_in_end(obj,obj_infs,pd_log_id)
            client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
        else:
            if check_time_window == 0:
                print "\nWARNING: The obs time is over!"
                time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                pg_act('pd_log_current','update',[{'obj_comp_time':time_now,'obs_stag':'pass'},{'id':pd_log_id}])
                update_to_db_in_end(obj,obj_infs,pd_log_id)
                client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])

def check_block(obj, pd_log_id):
    res = pg_act('pd_log_current','select',[['obs_stag','obj_comp_time'],{'obj_id':obj,'obs_stag':'sent'}, 'ORDER BY id DESC LIMIT 1'])
    if res:
        obs_stag, obj_comp_time = res[0][:]
        if obs_stag == 'complete':
            return 1
        elif check_time_window(obj) == 0:
            if group_id == 'XL002':
                if unit_id == '001':
                    cam_id = '1'
                    path = '/home/w60ccd/pd-socket'
                elif unit_id == '002':
                    cam_id = '2'
                    path = '/home/e60ccd/pd-socket'
            elif group_id == 'XL003':
                cam_id = '3'
                path = '/home/ccduser/pd-socket'
            ser_ip, ser_port = get_ser_config(group_id, cam_id)[:]
            cmd = "cd %s/ && nohup bash check_obs_stop.sh %s > /dev/null 2>&1" % (path, obj)
            pd_socket_client(ser_ip, ser_port, cmd)
            print '\n\033[0;31mWARNING:\033[0m The obj %s of %s is pass. ' % (obj, type_tel[group_id])
            #######
            log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            pg_act('pd_log_current','update',[{'obj_comp_time':log_com_time,'obs_stag':'complete'},{'id':pd_log_id}])
            # while True:
            #     if check_sent_update(pd_log_id,{'obj_comp_time':log_com_time,'obs_stag':'complete'}):
            #         break
            #######
            update_to_db_in_end(obj,obj_infs,pd_log_id)
            #######
            client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
            return 1
        else:
            return

def check_obj_status(obj,obj_infs,pd_log_id):
    group_id, obj_name, objra, objdec, filter, expdur, frmcnt, priority, obs_type = at(obj_infs, 'group_id', 'obj_name', 'objra', 'objdec', 'filter', 'expdur', 'frmcnt', 'priority', 'obs_type')
    type_tel = {'XL001':'GWAC', 'XL002':'F60', 'XL003':'F30'}
    stage = 'Sent'
    res = pg_act('pd_log_current','select',[['group_id','unit_id','obj_sent_time','obj_dist_id'],{'id':pd_log_id}])
    if res:
        group_id, unit_id, obj_sent_time, log_dist_id = res[0][:]
        if group_id in ['XL002','XL003']:
            if log_dist_id == '1':
                unit_id = '001'
            if log_dist_id == '2':
                unit_id = '002'
            if log_dist_id == '3':
                unit_id = '001'
    else:
        print '\nWARNING: There is no send inf about %s of %s when check_obj_status.' % (obj, type_tel[group_id])
        return
    if obj_infs['obs_type'] == 'cal':
        check_obj_stat_with_cal(obj,obj_infs,group_id,unit_id,obj_sent_time)
        return
    if group_id == 'XL001':
        ### check the gwac,gtoaes_log
        print "\nObj: %s %s %s %s %s %s %s %s %s." % (type_tel[group_id], unit_id, obj, obj_name, objra, objdec, str(int(float(expdur))), frmcnt, priority)
        #print "\nChecking it in log ..."
    if group_id in ['XL002','XL003']:
        ### check the gft,log_dist and cam_logs
        print "\nObj: %s %s %s %s %s %s %s %s %s %s." % (type_tel[group_id], unit_id, obj, obj_name, objra, objdec, filter, str(expdur), frmcnt, priority)
        if obj_infs['objsour'] == 'Block':
            if check_block(obj, pd_log_id):
                print '\nThe obj %s of %s is Complete.' % (obj, type_tel[group_id])
            else:
                print '\nThe obj %s of %s is Observing' % (obj, type_tel[group_id])
            return
        #print "\nChecking it in log ..."
    check_log_dist_res = check_log_dist(obj,pd_log_id)
    if check_log_dist_res:
        if type(check_log_dist_res) == type(''):
            print '\n\033[0;31mWARNING:\033[0m There is no send inf about %s of %s when check_obj_dist.' % (obj, type_tel[group_id])
            return
        if check_log_dist_res == 2:
            print '\n\033[0;31mWARNING:\033[0m The obj %s of %s is broken. ' % (obj, type_tel[group_id])
            ######
            time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            pg_act('pd_log_current','update',[{'obj_comp_time':time_now,'obs_stag':'break'},{'id':pd_log_id}])
            while True:
                if check_sent_update(pd_log_id,{'obj_comp_time':time_now,'obs_stag':'break'}):
                    break
            ######
            update_to_db_in_end(obj,obj_infs,pd_log_id)
            ######
            client.Send({"obj_id":obj,"obs_stag":'break'},['update','object_list_current','obs_stag'])
            return
        if check_log_dist_res == 1:
            stage = 'Observing'
    else: ### no check_log_dist_res
        res = pg_act('pd_log_current','select',[['obj_sent_time','obs_stag'],{'id':pd_log_id}])
        if res:
            log_sent_time, obs_stag = res[0][:]
            if obs_stag == 'sent':
                if check_time_window(obj) == 0:
                    print '\n\033[0;31mWARNING:\033[0m The obj %s of %s is pass. ' % (obj, type_tel[group_id])
                    #######
                    log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    pg_act('pd_log_current','update',[{'obj_comp_time':log_com_time,'obs_stag':'pass'},{'id':pd_log_id}])
                    while True:
                        if check_sent_update(pd_log_id,{'obj_comp_time':log_com_time,'obs_stag':'pass'}):
                            break
                    #######
                    update_to_db_in_end(obj,obj_infs,pd_log_id)
                    #######
                    client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                    return
                sent_delay = time.time() - time.mktime(time.strptime(log_sent_time, "%Y-%m-%d %H:%M:%S"))
                if  sent_delay > 1800: ### wait for 30 mins
                    #if group_id == 'XL003':
                     #   cam_ip, cam_port = get_cam_config('3')[:]
                      #  cmd = 'autostop && autostart'
                       # pd_socket_client(cam_ip, cam_port, cmd)
                    sent_delay_min = str(int(sent_delay/60))
                    print "\n%s \033[0;31mWARNING:\033[0m The obj has waited for %s mins after sent. Please check the system, then break current object or not." % (obj, sent_delay_min)
                    #time.sleep(3)
            else:
                print '\n\033[0;31mWARNING:\033[0m There is no send inf about %s of %s when check_obj_dist.' % (obj, type_tel[group_id])
                return
    if stage == 'Observing':
        check_cam_log_res = check_cam_log(obj,obj_infs,pd_log_id)
        if check_cam_log_res:
            if type(check_cam_log_res) == type(''):
                stage = 'break'
                print '\n\033[0;31mWARNING:\033[0m There is no send inf about %s of %s when check_obj_comp.' % (obj, type_tel[group_id])
                log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            elif type(check_cam_log_res) == type([]) and check_cam_log_res[0] == 2:
                stage = 'break'
                print '\n\033[0;31mWARNING:\033[0m The obj %s of %s is broken. ' % (obj, type_tel[group_id])
                log_com_time = check_cam_log_res[1]
            elif type(check_cam_log_res) == type([]) and check_cam_log_res[0] == 1:
                stage = 'complete'
                print '\nThe obj %s of %s is complete. ' % (obj, type_tel[group_id])
                log_com_time = check_cam_log_res[1]
            else:
                if type(check_cam_log_res) == type([]) and check_cam_log_res[0] == 0:
                    stage = 'proc'
                    proc = check_cam_log_res[1] #'Observing/%s' % proc
                    print '\nThe obj %s of %s is Observing: %s' % (obj,type_tel[group_id], proc)
                res = pg_act('pd_log_current','select',[['obj_dist_time','obs_stag'],{'id':pd_log_id}])
                if res:
                    log_dist_time, obs_stag = res[0][:]
                    if obs_stag == 'sent':
                        if check_time_window(obj) == 0:
                            print "\n%s \033[0;31mWARNING:\033[0m The observation time is over !" % obj
                            log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                            #######
                            pg_act('pd_log_current','update',[{'obj_comp_time':log_com_time,'obs_stag':'pass'},{'id':pd_log_id}])
                            while True:
                                if check_sent_update(pd_log_id,{'obj_comp_time':log_com_time,'obs_stag':'pass'}):
                                    break
                            update_to_db_in_end(obj,obj_infs,pd_log_id)
                            client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                            #######
                            return
                        if group_id in ['XL002','XL003']:
                            dist_delay = time.time() - time.mktime(time.strptime(log_dist_time, "%Y-%m-%d %H:%M:%S"))
                            if dist_delay > 1800:### wait for 30 mins
                                # if group_id == 'XL003':
                                #     cam_ip, cam_port = get_cam_config('3')[:]
                                #     cmd = 'autostop && autostart'
                                #     pd_socket_client(cam_ip, cam_port, cmd)
                                dist_delay_min = str(int(dist_delay/60))
                                print "\n%s \033[0;31mWARNING:\033[0m The obj has waited for %s mins after dist. Please check the system, then break current object or not." % (obj, dist_delay_min)
                                #time.sleep(3)
                    else:
                        print '\n\033[0;31mWARNING:\033[0m There is no send inf about %s of %s when check_obj_comp.' % (obj, type_tel[group_id])
                        return
            if stage in ['break','complete']:
                pg_act('pd_log_current','update',[{'obj_comp_time':log_com_time,'obs_stag':stage},{'id':pd_log_id}])
                while True:
                    if check_sent_update(pd_log_id,{'obj_comp_time':log_com_time,'obs_stag':stage}):
                        break
                ######
                update_to_db_in_end(obj,obj_infs,pd_log_id)
                ######
                client.Send({"obj_id":obj,"obs_stag":stage},['update','object_list_current','obs_stag'])
                return
    else:
        print '\nThe obj %s of %s is sent, not distributed.' % (obj, type_tel[group_id])

def get_pd_log_id(obj):
    #res = pg_act('pd_log_current','select',[['id'],{'obj_id':obj,'obs_stag':'sent'},'ORDER BY id DESC LIMIT 1'])
    res = pg_act('pd_log_current','select',[['id'],{'obj_id':obj,'obs_stag':'sent'},'ORDER BY id'])
    if res:
        if len(res) == 1:
            pd_log_id = str(res[0][0])
        else:
            for i in range(len(res)-1):
                pg_act('pd_log_current','update',[{'obs_stag':'wrong'},{'id':str(res[i][0])}])
                while True:
                    if check_sent_update(i[0],{'obs_stag':'wrong'}):
                        break
            pd_log_id = str(res[len(res)-1][0])
        return pd_log_id

def check_sent():
    while True:
        gwac_sent_objs = get_sent_indb('XL001')
        f60_sent_objs = get_sent_indb('XL002')
        f30_sent_objs = get_sent_indb('XL003')
        if gwac_sent_objs:
            print "\n\n\nCurrent objs of GWAC: " + ','.join(gwac_sent_objs)
            for obj in gwac_sent_objs:
                obj_infs = get_obj_infs(obj)
                if not obj_infs:
                    continue
                pd_log_id = str(get_pd_log_id(obj))
                check_obj_status(obj,obj_infs,pd_log_id)
                print '\n'
        if f60_sent_objs:
            print "\n\n\nCurrent objs of F60: " + ','.join(f60_sent_objs)
            for obj in f60_sent_objs:
                obj_infs = get_obj_infs(obj)
                if not obj_infs:
                    continue
                pd_log_id = str(get_pd_log_id(obj))
                check_obj_status(obj,obj_infs,pd_log_id)
                print '\n'
        if f30_sent_objs:
            print "\n\n\nCurrent objs of F30: " + ','.join(f30_sent_objs)
            for obj in f30_sent_objs:
                obj_infs = get_obj_infs(obj)
                if not obj_infs:
                    continue
                pd_log_id = str(get_pd_log_id(obj))
                check_obj_status(obj,obj_infs,pd_log_id)
                print '\n'

def check_sent_main(group_id):
    type_tel = {'XL001':'GWAC', 'XL002':'F60', 'XL003':'F30'}
    sent_objs = get_sent_indb(group_id)
    if sent_objs:
        print "\n\n\nCurrent objs of %s: " % type_tel[group_id] + ','.join(sent_objs)
        for obj in sent_objs:
            obj_infs = get_obj_infs(obj)
            if not obj_infs:
                continue
            pd_log_id = str(get_pd_log_id(obj))
            check_obj_status(obj,obj_infs,pd_log_id)
            print '\n'

def check_sent_new():
    while True:
        t1 = threading.Thread(target=check_sent_main,args=('XL001',))
        t1.setDaemon(True)
        t1.start()
        t2 = threading.Thread(target=check_sent_main,args=('XL002',))
        t2.setDaemon(True)
        t2.start()
        t3 = threading.Thread(target=check_sent_main,args=('XL003',))
        t3.setDaemon(True)
        t3.start()
        time.sleep(1)
        t1.join()
        t2.join()
        t3.join()

def check_sent_main_new(obj):
    #imp.acquire_lock()
    obj_infs = get_obj_infs(obj)
    if not obj_infs:
        return
    pd_log_id = str(get_pd_log_id(obj))
    check_obj_status(obj,obj_infs,pd_log_id)
    #imp.release_lock()

def check_sent_new_new():
    while True:
        gwac_sent_objs = get_sent_indb('XL001')
        f60_sent_objs = get_sent_indb('XL002')
        f30_sent_objs = get_sent_indb('XL003')
        if gwac_sent_objs:
            print "\nCurrent objs of GWAC: " + ','.join(gwac_sent_objs)
        if f60_sent_objs:
            print "\nCurrent objs of F60: " + ','.join(f60_sent_objs)
        if f30_sent_objs:
            print "\nCurrent objs of F30: " + ','.join(f30_sent_objs)
        ###
        sent_objs = gwac_sent_objs + f60_sent_objs + f30_sent_objs
        if sent_objs:
            t_pool = []
            for obj in sent_objs:
                t_cn = threading.Thread(target=check_sent_main_new, args=(obj,))
                t_pool.append(t_cn)
            for t in t_pool:    
                t.setDaemon(True)
                t.start()
            for t in t_pool:
                t.join()
            print '\n\n\n'

def get_msg():
    client.Recv()
    data = client.recv_data
    if data["content"]:
        msg = data["content"]
        return msg
    else:
        return 0

def get_sent_indb(group_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag='sent' and object_list_all.group_id='"+ group_id +"' AND object_list_current.mode='observation' ORDER BY object_list_current.id"
    res = sql_act(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs

def get_new_indb(group_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag in ('scheduled','observable') and object_list_all.group_id='"+ group_id +"' AND object_list_current.mode='observation' ORDER BY object_list_current.id"
    res = sql_act(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs

def get_com_indb(group_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag in ('complete','break','pass') and object_list_all.group_id='"+ group_id +"' AND object_list_current.mode='observation' ORDER BY object_list_current.id"
    res = sql_act(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs

def get_uesd_teles_from_db(group_id):
    used_units = []
    pri_list = []
    date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    res = pg_act('pd_log_current','select',[['unit_id','priority'],{'obs_stag':'sent','date_cur':date_cur,'group_id':group_id}])
    if res:
        res.sort(key=operator.itemgetter(1))
        unit_pri = res
        res_dic_set_lis = sorted(dict(res).items(), key=operator.itemgetter(1))
        for k, v in res_dic_set_lis:
            used_units.append(k)
            pri_list.append(v)
        return [unit_pri,pri_list,used_units]
    else:
        return 0

def pre_units(group_id,obj_infs):
    used_sts = get_uesd_teles_from_db(group_id)
    #print used_sts
    if used_sts:
        unit_pri, pri_list, used_units= used_sts[:]
    else:
        unit_pri = []
        pri_list = ['0']
        used_units = []
    if group_id == 'XL001':
        if 'GW' in str(obj_infs['objsour']):
            initunits = load_params('./pd_params.json')['obs']['GWAC_init_for_GW']
        else:
            initunits = load_params('./pd_params.json')['obs']['GWAC_init']
    if group_id == 'XL002':
        initunits = load_params('./pd_params.json')['obs']['F60_init']
    if group_id == 'XL003':
        initunits = load_params('./pd_params.json')['obs']['F30_init']
    if not used_units:
        unused_units = initunits[:]
    else:
        unused_units = list(set(initunits) - set(used_units))
    for_new_units = unused_units + used_units
    return [initunits, unit_pri, unused_units, for_new_units, pri_list]

def check_new_main(group_id):
    global send_objs
    mark_wait = 0
    type_tel = {'XL001':'GWAC', 'XL002':'F60', 'XL003':'F30'}

    new_objs = get_new_indb(group_id)
    com_objs = get_com_indb(group_id)

    # log_file.write('\n##### %s_new_objs(%s): ' % (type_tel[group_id] ,str(len(new_objs))) + ','.join(new_objs))
    #print '\n##### %s_new_objs: %s ' % (type_tel[group_id] ,str(len(new_objs)))# + ','.join(new_objs)
    log_file.write('\n##### %s_com_objs(%s): ' % (type_tel[group_id] ,str(len(com_objs))) + ','.join(com_objs))
    print '\n##### %s_com_objs: %s ' % (type_tel[group_id] ,str(len(com_objs)))# + ','.join(com_objs)
    if new_objs:
        log_file.write('\n##### %s : Case 1' % type_tel[group_id])
        for obj in new_objs:
            if obj not in send_objs:
                send_objs.append(obj)
                obj_infs = get_obj_infs(obj)
                if not obj_infs:
                    continue
                initunits, unit_pri, unused_units, for_new_units, pri_list = pre_units(group_id,obj_infs)[:]
                obj_pri = obj_infs['priority']
                #print pri_list
                if int(obj_pri) > int(pri_list[0]):
                    units = for_new_units
                    if pri_list[0] == '0':
                        print '\n##### %s Common, %s ' % (type_tel[group_id], obj_pri)
                    else:
                        print '\n##### %s Higher Priority, %s ' % (type_tel[group_id], obj_pri)
                else:
                    if group_id in ['XL002'] and obj_infs['obs_type'] != 'cal':
                        if len(unit_pri) < len(initunits) + 1:
                            units = unused_units + [for_new_units[0]]
                        else:
                            units = unused_units
                    else:
                        units = unused_units
                    print '\n##### %s Common, %s ' % (type_tel[group_id], obj_pri)
                if units:
                    #print units
                    unit_id = obj_infs['unit_id']
                    if len(unit_id) > 3:
                        unit_id = units[0]
                    if unit_id in units:
                        time_res = check_time_window(obj)
                        if time_res == 1:
                            log_file.write('\n###### The obj %s of %s: Sending.' % (obj,type_tel[group_id]))
                            print '\n###### The obj %s of %s: Sending.' % (obj,type_tel[group_id])
                            send_beg_time, send_end_time, pd_log_id = send_obj(obj,obj_infs,group_id,unit_id)[:]
                            for it in range(5):
                                if check_log_sent(obj, send_beg_time, send_end_time, pd_log_id):
                                    check_log_sent_res = 1
                                    break
                                else:
                                    check_log_sent_res = 0
                                    time.sleep(it)
                            if check_log_sent_res == 1:
                                log_file.write("\n##### The obj %s of %s: Send ok." % (obj,type_tel[group_id]))
                                print "\n##### The obj %s of %s: Send ok." % (obj,type_tel[group_id])
                                client.Send({"obj_id":obj,"obs_stag":'sent'},['update','object_list_current','obs_stag'])
                                insert_to_db_in_beg(obj,obj_infs,pd_log_id)
                                send_objs.remove(obj)
                                time.sleep(0.5)
                            else:
                                print "\n##### %s WARNING: The obj of %s: Send Wrong." % (obj, type_tel[group_id])
                                log_file.write("\n##### The obj %s of %s: Send Wrong." % (obj, type_tel[group_id]))
                                pg_act('pd_log_current','update',[{'obs_stag':'resend'},{'obj_id':obj,'obs_stag':'sent'}])
                                send_objs.remove(obj)
                                if obj == new_objs[-1]:
                                    mark_wait = 1
                                    break
                        elif time_res == 0:
                            client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                            print "\n##### The obj %s of %s: Pass Ok." % (obj, type_tel[group_id])
                            log_file.write("\n##### The obj %s of %s: Pass Ok." % (obj, type_tel[group_id]))
                            send_objs.remove(obj)
                            break
                        else:
                            print "\n##### %s WARNING: The obj of %s: Need wait." % (obj, type_tel[group_id])
                            log_file.write("\n##### The obj %s of %s: Need Wait." % (obj, type_tel[group_id]))
                            mark_wait = 1
                            send_objs.remove(obj)
                            break
                else:
                    print "\n##### %s WARNING: There is no free units of %s, don't need to send." % (type_tel[group_id],obj)
                    log_file.write("\n##### There is no free units of %s when send %s, don't need to send." % (type_tel[group_id],obj))
                    send_objs.remove(obj)
                    break
                # if obj in send_objs:
                #     send_objs.remove(obj)
            else:
                print '\n##### %s : Case Wrong. ' % type_tel[group_id]
                break
    else:
        log_file.write('\n##### %s : Case 2' % type_tel[group_id])
    return mark_wait


def get_new_indb_new(group_id,unit_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag in ('scheduled','observable') and object_list_all.group_id='"+ group_id +"' AND (unit_id='"+ unit_id +"' OR (unit_id LIKE '%|%'  AND unit_id LIKE '%"+ unit_id +"%' )) AND object_list_current.mode='observation' ORDER BY object_list_current.id LIMIT 10"
    res = sql_act(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs

def get_uesd_teles_from_db_by_gu(group_id,unit_id):
    pri_list = []
    obj_list = []
    date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    res = pg_act('pd_log_current','select',[['priority','obj_id'],{'obs_stag':'sent','date_cur':date_cur,'group_id':group_id,'unit_id':unit_id}])
    if res:
        for i in res:
            pri_list.append(i[0])
            obj_list.append(i[1])
        pri_list.sort()
        obj_list = list(set(obj_list))
    return [pri_list, obj_list]

def check_tw_end(obj_list):
    for obj in obj_list:
        time_cur_mark = time.mktime(datetime.datetime.utcnow().timetuple())
        res = pg_act('object_list_current','select',[['tw_end'],{'obj_id':obj}])
        if res:
            tw_end = res[0][0]
            tw_end_mark = time.mktime(time.strptime(tw_end, "%Y/%m/%d %H:%M:%S"))
            if (float(tw_end_mark) - float(time_cur_mark)) <= 1800:
                return 1
    return 0

def check_new_new_main(group_id):
    global send_objs
    mark_wait = 0
    type_tel = {'XL001':'GWAC', 'XL002':'F60', 'XL003':'F30'}
    #com_objs = get_com_indb(group_id)
    initunits = load_params('./pd_params.json')['obs'][type_tel[group_id]+'_init']
    if initunits:
        # log_file.write('\n##### %s_com_objs(%s): ' % (type_tel[group_id] ,str(len(com_objs))) + ','.join(com_objs))
        # print '\n##### %s_com_objs: %s ' % (type_tel[group_id] ,str(len(com_objs)))# + ','.join(com_objs)
        for unit_id_init in initunits:
            new_objs = get_new_indb_new(group_id,unit_id_init)
            if new_objs:
                log_file.write('\n##### %s : Case 1' % type_tel[group_id])
                for obj in new_objs:
                    if obj not in send_objs:
                        send_objs.append(obj)
                        obj_infs = get_obj_infs(obj)
                        if not obj_infs:
                            continue
                        unit_id = unit_id_init
                        if group_id == 'XL001' and ('GW' in obj_infs['objsour']) and (unit_id not in load_params('./pd_params.json')['obs']['GWAC_init_for_GW']): ###默认 GWAC_init_for_GW 少于 GWAC_init
                            if obj in send_objs:
                                send_objs.remove(obj)
                            break
                        pri_list, obj_sent_list = get_uesd_teles_from_db_by_gu(group_id,unit_id)[:]
                        #print obj_pri, pri_list
                        obj_pri = obj_infs['priority']
                        go_sent = 0
                        if pri_list:
                            if int(obj_pri) > int(pri_list[-1]):
                                if not check_tw_end(obj_sent_list):
                                    print '\n##### %s %s: Higher Priority, %s, %s' % (type_tel[group_id], unit_id, obj, obj_pri)
                                    go_sent = 1
                                    if group_id in ['XL002','XL003']:
                                        sent_objs = get_sent_indb(group_id)
                                        for sent_obj in sent_objs:
                                            sent_obj_infs = get_obj_infs(sent_obj)
                                            if not sent_obj_infs:
                                                continue
                                            if sent_obj_infs['objsour'] == 'Block':
                                                if group_id == 'XL002':
                                                    if unit_id == '001':
                                                        cam_id = '1'
                                                        path = '/home/w60ccd/pd-socket'
                                                    elif unit_id == '002':
                                                        cam_id = '2'
                                                        path = '/home/e60ccd/pd-socket'
                                                elif group_id == 'XL003':
                                                    cam_id = '3'
                                                    path = '/home/ccduser/pd-socket'
                                                ser_ip, ser_port = get_ser_config(group_id, cam_id)[:]
                                                cmd = "cd %s/ && nohup bash check_obs_stop.sh %s > /dev/null 2>&1" % (path, sent_obj)
                                                pd_socket_client(ser_ip, ser_port, cmd)
                                                log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                                                pg_act('pd_log_current','update',[{'obj_comp_time':log_com_time,'obs_stag':'complete'},{'obj_id':sent_obj,'obs_stag':'sent'}])
                                                pd_log_id = str(get_pd_log_id(sent_obj))
                                                update_to_db_in_end(sent_obj,sent_obj_infs,pd_log_id)
                                                #######
                                                client.Send({"obj_id":sent_obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
                            else:
                                if group_id == 'XL002' and unit_id == '001' and len(pri_list) < 2:
                                    print '\n##### %s %s: Common, %s, %s' % (type_tel[group_id], unit_id, obj, obj_pri)
                                    go_sent = 1
                        else:
                            print '\n##### %s %s: Common, %s, %s ' % (type_tel[group_id], unit_id, obj, obj_pri)
                            go_sent = 1
                        if go_sent:
                            time_res = check_time_window(obj)
                            if time_res == 1:
                                log_file.write('\n###### The obj %s of %s: Sending.' % (obj,type_tel[group_id]))
                                print '\n###### The obj %s of %s: Sending.' % (obj,type_tel[group_id])
                                send_beg_time, send_end_time, pd_log_id = send_obj(obj,obj_infs,group_id,unit_id)[:]
                                for it in range(5):
                                    check_log_sent_res = 0
                                    if obj_infs['objsour'] == 'Block':
                                        check_log_sent_res = 1
                                        break
                                    if check_log_sent(obj, send_beg_time, send_end_time, pd_log_id):
                                        check_log_sent_res = 1
                                        break
                                    else:
                                        check_log_sent_res = 0
                                        time.sleep(1)
                                if check_log_sent_res == 1:
                                    log_file.write("\n##### The obj %s of %s: Send ok." % (obj,type_tel[group_id]))
                                    print "\n##### The obj %s of %s: Send ok." % (obj,type_tel[group_id])
                                    client.Send({"obj_id":obj,"obs_stag":'sent'},['update','object_list_current','obs_stag'])
                                    insert_to_db_in_beg(obj,obj_infs,pd_log_id)
                                    if obj in send_objs:
                                        send_objs.remove(obj)
                                    if group_id == 'XL002':
                                        if len(pri_list) >= 1:
                                            break
                                    else:
                                        break
                                else:
                                    print "\n##### %s WARNING: The obj of %s: Send Wrong." % (obj, type_tel[group_id])
                                    log_file.write("\n##### The obj %s of %s: Send Wrong." % (obj, type_tel[group_id]))
                                    pg_act('pd_log_current','update',[{'obs_stag':'resend'},{'obj_id':obj,'obs_stag':'sent'}])
                                    if obj in send_objs:
                                        send_objs.remove(obj)
                                    if obj == new_objs[-1]:
                                        mark_wait = 1
                                        break
                            elif time_res == 0:
                                client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                                print "\n##### The obj %s of %s: Pass Ok." % (obj, type_tel[group_id])
                                log_file.write("\n##### The obj %s of %s: Pass Ok." % (obj, type_tel[group_id]))
                                if obj in send_objs:
                                    send_objs.remove(obj)
                                break
                            else:
                                print "\n##### %s WARNING: The obj of %s: Need wait." % (obj, type_tel[group_id])
                                log_file.write("\n##### The obj %s of %s: Need Wait." % (obj, type_tel[group_id]))
                                mark_wait = 1
                                if obj in send_objs:
                                    send_objs.remove(obj)
                                break
                        else:
                            print "\n##### %s %s: Don't need to send." % (type_tel[group_id],unit_id)
                            log_file.write("\n##### There is no free units of %s when send %s, don't need to send." % (type_tel[group_id],obj))
                            if obj in send_objs:
                                send_objs.remove(obj)
                            break
                    else:
                        print '\n##### %s : Case 3. ' % type_tel[group_id]
                        log_file.write('\n##### %s : Case 3' % type_tel[group_id])
                        break
            else:
                log_file.write('\n##### %s : Case 2' % type_tel[group_id])
    #return mark_wait
    if mark_wait:
        time.sleep(10)
        client_og.Send("Hello World",['insert'])
    return mark_wait

def sync():
    while True:
        time.sleep(0.5)
        mark = 0
        objs1 = []
        objs2 = []
        pd_log_tab = 'pd_log_current'
        sql = "SELECT obj_id FROM object_list_current WHERE obs_stag in ('sent') AND mode='observation'"
        res = sql_act(sql)
        if res:
            objs1 = res
            #print objs1
        sql = "SELECT obj_id FROM "+pd_log_tab+" WHERE obs_stag in ('complete', 'pass', 'break')"
        res = sql_act(sql)
        if res:
            objs2 = res
            #print objs2
        for obj in objs2:
            if obj in objs1:
                mark = 1
        if mark == 0:
            break

class Mythread_get_result(threading.Thread):
    def __init__(self, target=None, args=(),kwargs={}):
        super(Mythread_get_result,self).__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.__result = 0
    def run(self):
        self.__result = self.target(*self.args, **self.kwargs)
    def get_result(self):
        return self.__result

class Mythread_wait_and_sendmsg(threading.Thread):
    def __init__(self):
        super(Mythread_wait_and_sendmsg,self).__init__()
        self.__stop_flag = threading.Event()
        self.__stop_flag.set()
    def run(self):
        for i in range(60):
            if self.__stop_flag.isSet():
                time.sleep(0.5)
            else:
                break
        if self.__stop_flag.isSet():
            client_og.Send("Hello World",['insert'])
            self.__stop_flag.clear()
    def stop_it(self):
        if self.__stop_flag.isSet():
            self.__stop_flag.clear()
            self.join()

def check_new():
    t_wasm = None
    while True:
        msg = get_msg()
        if msg:
            time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            print '\n###### Get news (%s)' % time_now
            log_file.write("\n###### Get news (%s) ######" % time_now)
            #sync()
            if t_wasm:
                if t_wasm.isAlive():
                    t_wasm.stop_it()
                t_wasm = None
            mark_wait = 0
            t_cn1 = Mythread_get_result(target=check_new_new_main,args=('XL003',))
            t_cn2 = Mythread_get_result(target=check_new_new_main,args=('XL002',))
            t_cn3 = Mythread_get_result(target=check_new_new_main,args=('XL001',))
            t_cn1.setDaemon(True)
            t_cn1.start()
            t_cn2.setDaemon(True)
            t_cn2.start()
            t_cn3.setDaemon(True)
            t_cn3.start()
            # t_cn1.join()
            # t_cn2.join()
            # t_cn3.join()
            # mark_wait1 = t_cn1.get_result()
            # mark_wait2 = t_cn2.get_result()
            # mark_wait3 = t_cn3.get_result()
            # mark_wait = mark_wait1 + mark_wait2 + mark_wait3
            # for group_id in ['XL001','XL002','XL003']:
            #     print '\n\n\n'
            #     k = check_new_main(group_id)
            #     mark_wait += k
            if mark_wait:
                t_wasm = Mythread_wait_and_sendmsg()
                t_wasm.setDaemon(True)
                t_wasm.start()
            print '\n###### Ready to get news.'
            log_file.write('\n###### Ready to get news ######')


if __name__ == "__main__":
    import _strptime
    print '\nInit DB ...'
    db_init()
    time.sleep(1)
    print '\nInit Daemon ...'
    t_init = threading.Thread(target=pd_init)
    t_init.setDaemon(True)
    t_init.start()
    time.sleep(1)
    thread_inif30 = threading.Thread(target=check_F30_sync)
    thread_inif30.setDaemon(True)
    thread_inif30.start()
    time.sleep(1)
    thread_floup = threading.Thread(target=check_ser_socket_background)
    thread_floup.setDaemon(True)
    thread_floup.start()
    time.sleep(1)
    #print '\nCal ...'
    #####
    print '\nGoing...'
    thread_floup = threading.Thread(target=pd_followup)
    thread_floup.setDaemon(True)
    thread_floup.start()
    time.sleep(1)
    #thread_main = threading.Thread(target=check_all_device)
    #thread_main.setDaemon(True)
    #thread_main.start()
    #time.sleep(1)
    thread_main = threading.Thread(target=check_sent_new_new)
    thread_main.setDaemon(True)
    thread_main.start()
    time.sleep(1)
    check_new()
