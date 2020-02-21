#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import re, os, sys, time, datetime, threading, psycopg2
from ToP_obs_plan_insert_DB import update_to_ba_db,update_pointing_lalalert

def con_db():
    host = '172.28.8.28'#'10.0.10.236'
    database = 'gwacyw'
    user = 'yunwei'
    password = 'gwac1234'
    try:
        db = psycopg2.connect(host=host, port=5432, user=user, password=password, database=database)
        return db
    except psycopg2.Error as e:
        print(e)
        return False

def sql_get(sql, n=1):
    db = con_db()
    if db:
        cur = db.cursor()
        try:
            if n == 0:###仅执行sql
                cur.execute(sql)
                db.commit()
                time.sleep(1.5)
            else:
                cur.execute(sql)
                rows = cur.fetchall()
                return rows
        except psycopg2.Error as e:
            print("\nWARNING: Wrong with operating the db, %s " % str(e).strip())
            return False
        finally:
            db.close()
    else:
        print("\nWARNING: Connection to the db is Error.")

def pg_db(table,action,args=[]):
    if args:
        if action == "update":
            #args = [{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'},{'obj_id':'1','obs_stag':'sent'}]
            rows = args[0].keys()
            targs = []
            for row in rows:
                targs.append("='".join([row,args[0][row]]))
            targ = "' , ".join(targs)
            cond_keys = args[1].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[1][key]]))
            cond = "' AND ".join(conds)
            sql = "UPDATE " + table + " SET " + targ + "' WHERE " + cond + "'"
            #print sql
            sql_get(sql, 0)
        if action == "select":
            #args = [['1','2'],{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'}]
            rows = ','.join(args[0])
            if args[1]:
                cond_keys = args[1].keys()
                conds = []
                for key in cond_keys:
                    conds.append("='".join([key,args[1][key]]))
                cond = "' AND ".join(conds)
                cond += "'"
            else:
                cond = ''
            if len(args) > 2:
                cond_more = args[2]
            else:
                cond_more = ''
            sql = "SELECT " + rows + " FROM " + table + " WHERE " + cond + cond_more
            #print sql
            res = sql_get(sql)
            return res

def Ra_to_hms(i):
    if ':' in i:
        RaList = map(float, i.split(':'))
        if RaList[0] >= 0 and RaList[0] < 24 and RaList[1] < 60 and RaList[2] < 60:
            return i
    elif abs(float(i)) < 360:
        a = float(i)/15
        b = (float(i)-15*int(a))*4
        c = (b-int(b))*60
        if len(str(int(c))) > 1:
            return '%02d:%02d:%.1f' % (a,b,c)
        else:
            return '%02d:%02d:0%.1f' % (a,b,c)
    else:
        print '\nWARNING: Wrong RA, please input hms or degree'

def get_obj_infs(obj):            ### Get the infomation.
    sql = "SELECT group_id, unit_id, obj_name, objra, objdec, filter, expdur, frmcnt, priority, objsour FROM object_list_all WHERE obj_id='" + obj + "'"
    infs = list(sql_get(sql)[0])
    infs[-3] = str(int(infs[-3]))
    if len(infs[2]) > 20:
        if infs[0] in ['XL002', 'XL003']:
            infs[2] = infs[2][:20]
            infs[3] = Ra_to_hms(str(infs[3]))
    if infs[5] == 'clear':
        if infs[0] == 'XL002':
            infs[5] = 'Lum'
        if infs[0] == 'XL003':
            print "\nWARNING: The filter of %s input Error, using filter R." % obj
            infs[5] = 'R'
    return infs

def send_db_in_end(obj):
    # pd_log_tab = 'pd_log_current'
    # running_list_cur = 'object_running_list_current'
    infs = get_obj_infs(obj)
    objsource = infs[9]
    obj_name = infs[2]
    obs_stag = ''
    #time.sleep(1.5)
    res = pg_db('pd_log_current','select',[['id','group_id','unit_id','obj_dist_id','obj_comp_time','obs_stag'],{'obj_id':obj},'ORDER BY id DESC LIMIT 1'])
    if res:
        pd_id, group_id, unit_id, log_dist_id, obj_comp_time,obs_stag = res[0][:]
        if group_id in ['XL002','XL003']:
            if log_dist_id == '1':
                unit_id = '001'
            if log_dist_id == '2':
                unit_id = '002'
            if log_dist_id == '3':
                unit_id = '001'
    else:
        return
    if not obj_comp_time:
        obj_comp_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
    if obs_stag == 'complete':
        up_stag = obs_stag
    elif obs_stag == 'break':
        up_stag = 'broken'
    elif obs_stag == 'pass':
        up_stag = 'pass obs time window'
    elif obs_stag == 'sent':
        pg_db('pd_log_current','update', [{'obj_comp_time':obj_comp_time,'obs_stag':'break'},{'id':pd_id}])
        obs_stag = 'break'
        up_stag = 'broken'
    else:
        obs_stag = 'break'
        up_stag = 'broken'
    e_time = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(obj_comp_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
    endTime = e_time.replace('T',' ')
    #####
    pg_db('object_running_list_current','update', [{'end_time':e_time,'unit_id':unit_id},{'obj_id':obj}])
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
                    else:
                        grid_id = strs[0]
                        field_id = strs[1]
                else:
                    #grid_id = 0
                    field_id = obj_name
                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_name + "'" + 'and objsour=' + "'" + objsource + "'"
                res = sql_get(sql)
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
                res = sql_get(sql)
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
                res = sql_get(sql)
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
                            res = sql_get(sql)
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

def get_sent_indb(type):
    res_list = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id and object_list_current.obs_stag='sent' and group_id='" + type + "' ORDER BY object_list_current.id"
    #print sql
    res = sql_get(sql)
    if res:
        #print res
        for i in res:
            res_list.append(i[0])
        return res_list
    else:
        return []

def abandon_objs():
    from communication_client import Client
    s = Client('object_generator')
    gwac_sent = get_sent_indb('XL001')
    f60_sent = get_sent_indb('XL002')
    f30_sent = get_sent_indb('XL003')
    #print gwac_sent,f60_sent,f30_sent
    n = 0
    k = {}
    print '\nAbout: Obj_ID Unit_ID Obj_Name Ra Dec Filter Expdur Frmcnt Priority'
    print "\nGWAC：" 
    if gwac_sent:
        for i in gwac_sent:
            n += 1
            k[str(n)] = i
            infs = get_obj_infs(i)
            res = pg_db('pd_log_current','select',[['unit_id'],{'obj_id':i,'obs_stag':'sent'}])
            if res:
                unit_id = res[0][0]
            else:
                unit_id = 'None'
            print('    '+str(n)+': '+i+' '+unit_id+' '+', '.join([str(infs[i]) for i in range(2,10)]))
    else:
        print('    '+'None')
    
    print "\nF60："
    if f60_sent:
        for i in f60_sent:
            n += 1
            k[str(n)] = i
            infs = get_obj_infs(i)
            res = pg_db('pd_log_current','select',[['unit_id'],{'obj_id':i,'obs_stag':'sent'}])
            if res:
                unit_id = res[0][0]
            else:
                unit_id = 'None'
            print('    '+str(n)+': '+i+' '+unit_id+' '+', '.join([str(i) for i in infs[2:10]]))
    else:
        print('    '+'None')

    print "\nF30："
    if f30_sent:
        for i in f30_sent:
            n += 1
            k[str(n)] = i
            infs = get_obj_infs(i)
            res = pg_db('pd_log_current','select',[['unit_id'],{'obj_id':i,'obs_stag':'sent'}])
            if res:
                unit_id = res[0][0]
            else:
                unit_id = 'None'
            print('    '+str(n)+': '+i+' '+unit_id+' '+', '.join([str(i) for i in infs[2:10]]))
    else:
        print('    '+'None')
    if (gwac_sent+f60_sent+f30_sent):
        #print k
        print '\n\nPlease choose the order number which you want to break'
        print '    Example: 1  << Type "0" to abandon them all; Type "Enter" to quit >>'
        while True:
            inpt = raw_input(':')
            inpt = inpt.strip()
            if inpt in k.keys():
                break
            elif inpt == '0':
                print '\nAbandon them all.'
                break
            elif not inpt:
                exit('\nNothing done.\n')
            else:
                print '\nWARNING: Input wrong. Please input once again.'
        for i in range(len(k.keys())):
            if inpt != '0':
                obj = k[inpt]
            else:
                obj = k[str(i+1)]
            log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            pg_db('pd_log_current','update',[{'obj_comp_time':log_com_time,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
            send_db_in_end(obj)
            s.Send({"obj_id":obj,"obs_stag":'break'},['update','object_list_current','obs_stag'])
            if inpt != '0':
                break
            else:
                time.sleep(3)
        #s.Send("Hello World",['insert'])
    else:
        print '\nThere is no sent objs in db.'
    print '\nDone.\n'

if __name__ == "__main__":
    abandon_objs()
    # objs = ['114744','114727','114743','114742','114758','114726','114741','114725']
    # for obj in objs:
    #     send_db_in_end(obj)
