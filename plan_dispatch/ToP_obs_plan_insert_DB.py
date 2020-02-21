#!/usr/bin/env python
"""
Created on Wed Mar  6 03:37:28 2019

@author: Xuhui Han
"""

import sys
import json
import pymysql,psycopg2
import datetime
import time

#__________________________________
def CMM_DBConnect(location):
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        CMM_user = data["CMM_user_bj"]
    elif location == 'xinglong':
        CMM_user = data["CMM_user_xl"]
    try:
        db = pymysql.connect(data["CMM_host"],CMM_user,data["CMM_password"],data["CMM_db"] )
    except pymysql.Error as e:
        print e
        return False
    else:
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
    try:
        db=psycopg2.connect("host=" + yunwei_host + " port= " + data["yunwei_port"] + \
            " dbname='" + data["yunwei_db"] + "' user='" + data["yunwei_user"] + "' password='"  + data["yunwei_password"] + "'")
    except psycopg2.Error as e:
        print(e)
        return False
    else:
        return db
#__________________________________
def Yunwei_DBClose(db):
    "Close the connection to the DB"
    db.close()

def Retrieve_trigger_id(db,external_trigger_name):
    "Find the ID of the trigger type"
    cursor = db.cursor()
    query = "SELECT * FROM trigger_v where external_trigger_name = \'" + external_trigger_name + "\'"
    cursor.execute(query)
    results = cursor.fetchall()
    # print(results)
    if not results:
        ID_external_trigger = str(-1)
    else:
        ID_external_trigger = str(results[-1][0])
        # result_qry_json = []
        # d = {}
        # d['ID_external_trigger'] = results[-1][0]
        # d['external_trigger_name'] = results[-1][2]
        # d['alert_message_path'] = results[-1][46]
        # d['alert_message_revision'] = results[-1][47]
        # d['alert_message_type'] = results[-1][48]
        # result_qry_json = json.dumps(d, ensure_ascii=False)
    cursor.close()
    return ID_external_trigger

#__________________________________
def Retrieve_svom_observatory_telescope_instrument_ID(db,svom_telescope_name):
    "Find the ID of the svom telescope"
    cursor = db.cursor()
    query = "SELECT * FROM svom_telescope WHERE svom_telescope_name='"+str(svom_telescope_name)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        ID_svom_observatory = str(-1)
        ID_svom_telescope = str(-1)
        ID_svom_instrument = str(-1)
    else:
        ID_svom_telescope = str(results[-1][0])
        ID_svom_observatory = str(results[-1][4])
        query = "SELECT * FROM svom_instrument WHERE ID_SVOM_telescope='"+str(ID_svom_telescope)+"'"
        cursor.execute(query)
        result_inst = cursor.fetchall()
        ID_svom_instrument = str(result_inst[-1][0])
    
    return ID_svom_observatory,ID_svom_telescope, ID_svom_instrument

#__________________________________
def Retrieve_filter_ID(db,band,filter_system):
    "Find the ID of the filter used for the observation"
    cursor = db.cursor()
    if band=='clear':
        query = "SELECT * FROM filter WHERE filter_name='"+str(band)+"'"
    else:
        query = "SELECT * FROM filter WHERE filter_name='"+str(band)+"/"+str(filter_system)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        ID_filter = str(-1)
    else:
        ID_filter = str(results[-1][0])
    return ID_filter

#__________________________________
def func_top_obs_plan_inset_db(db,ID_observation_plan,ID_external_trigger,Telescope_ID_planned,Instrument_ID_planned,ID_filter_planned,Grid_ID_planned,Field_ID_planned,RA_planned,dec_planned,obs_strategy_planned,Tstart_planned,Tend_planned,exposure_planned,observation_plan_type,command_obs_status):
    object_list = "observation_plan"
    contact_name_planned = ''
    contact_mail_planned = ''
    Observatory_ID, ID_svom_telescope, ID_svom_instrument = Retrieve_svom_observatory_telescope_instrument_ID(db,Telescope_ID_planned)
    #print Observatory_ID,ID_svom_telescope,ID_svom_instrument
    filter_system = "Johnson"
    ID_filter = Retrieve_filter_ID(db,ID_filter_planned,filter_system)
    query = "INSERT INTO "  + object_list + \
    "( ID_observation_plan, " + \
    "ID_obsplan_GWAC_system, " + \
    "ID_external_trigger, " + \
    "ID_SVOM_observatory, " + \
    "ID_SVOM_telescope, " + \
    "ID_SVOM_instrument," + \
    "ID_filter," + \
    "ID_grid," + \
    "ID_field," + \
    "RA_center," + \
    "dec_center," + \
    "observation_type," + \
    "T_start," + \
    "T_end," + \
    "exposure," + \
    "duty_scientist_name," + \
    "duty_scientist_mail," + \
    "observation_plan_type," + \
    "observation_plan_command_obs_status) " + \
    "VALUES ( DEFAULT ,"+ \
    "'" + str(ID_observation_plan) + "'," + \
    "'" + str(ID_external_trigger) + "'," + \
    "'" + str(Observatory_ID) + "'," + \
    "'" + str(ID_svom_telescope) + "'," + \
    "'" + str(ID_svom_instrument) + "'," + \
    "'" + str(ID_filter) + "'," + \
    "'" + str(Grid_ID_planned) + "'," + \
    "'" + str(Field_ID_planned) + "'," + \
    "'" + str(RA_planned) + "'," + \
    "'" + str(dec_planned) + "'," + \
    "'" + str(obs_strategy_planned) + "'," + \
    "'" + str(Tstart_planned) + "'," + \
    "'" + str(Tend_planned) + "'," + \
    "'" + str(exposure_planned) + "'," + \
    "'" + str(contact_name_planned) + "'," + \
    "'" + str(contact_mail_planned) + "'," + \
    "'" + str(observation_plan_type) + "'," + \
    "'" + str(command_obs_status) + "'" + \
    ")"
    #print query
    try:
        cursor = db.cursor()  
        cursor.execute(query)
        db.commit()
        cursor.close()
    except Exception as e:
        print 'Error %s' % e
    return  

    # "observation_plan_command_schedule_status," + \

def insert_to_ba_db(objsour,op_sn,group_id,unit_id,filter,grid_id,field_id,ra,dec,obs_stra,begin_time,end_time,expdur,mode,obs_stag):
    location = 'xinglong'
    db = CMM_DBConnect(location)
    if db:
        objsour_word = objsour.split('_')
        if objsour_word[0] == 'GW':
            ID_observation_plan = op_sn
            trigger_id = objsour_word[2]
            try:
                trigger_id = int(trigger_id)
            except:
                trigger_id = Retrieve_trigger_id(db,trigger_id)
            else:
                trigger_id = str(trigger_id)
            if group_id == 'XL001' and unit_id == '001':
                Telescope_ID_planned = 'GWAC_unit1'
            elif group_id == 'XL001' and unit_id == '002':
                Telescope_ID_planned = 'GWAC_unit2'
            elif group_id == 'XL001' and unit_id == '003':
                Telescope_ID_planned = 'GWAC_unit3'
            elif group_id == 'XL001' and unit_id == '004':
                Telescope_ID_planned = 'GWAC_unit4'
            elif group_id == 'XL002' and unit_id == '001':
                Telescope_ID_planned = 'GWAC_F60_A'
            elif group_id == 'XL002' and unit_id == '002':
                Telescope_ID_planned = 'GWAC_F60_B'
            elif group_id == 'XL003' and unit_id == '001':
                Telescope_ID_planned = 'GWAC_F30'
            Instrument_ID_planned = unit_id
            ID_filter_planned = filter
            Grid_ID_planned = grid_id
            Field_ID_planned = field_id
            RA_planned = ra
            dec_planned = dec
            obs_strategy_planned = obs_stra
            Tstart_planned = begin_time
            Tend_planned = end_time
            exposure_planned = expdur
            observation_plan_type = mode
            command_obs_status = obs_stag
            func_top_obs_plan_inset_db(db,ID_observation_plan,trigger_id,Telescope_ID_planned,Instrument_ID_planned,ID_filter_planned,Grid_ID_planned,Field_ID_planned,RA_planned,dec_planned,obs_strategy_planned,Tstart_planned,Tend_planned,exposure_planned,observation_plan_type,command_obs_status)
        CMM_DBClose(db)

def update_to_ba_db(objsour, obj_id, obs_stag, end_time):
    location = 'xinglong'
    db = CMM_DBConnect(location)
    if db:
        objsour_word = objsour.split('_')
        if objsour_word[0] == 'GW':
            tab = "observation_plan" ## "ID_grid" "ID_field"
            query = "UPDATE " + tab + " SET observation_plan_command_obs_status='" + obs_stag + "', T_end='"+ end_time +"' WHERE ID_obsplan_GWAC_system='" + obj_id + "'"
            try:
                cursor = db.cursor()
                cursor.execute(query)
                db.commit()
                cursor.close()
            except Exception as e:
                print 'Error %s' % e
            finally:
                CMM_DBClose(db)

def update_pointing_lalalert(trigger_id,name_telescope,grid_id,field_id,grade,status):
    location = 'xinglong'
    db = CMM_DBConnect(location)
    if db:
        db_table = "pointing_lalalert"
        query = "UPDATE " + db_table + " SET pointing_status='" + status + "' WHERE ID_external_trigger='" + trigger_id + "' AND name_telescope='" + name_telescope + "' AND ID_grid='" + grid_id + "' AND ID_field='" + field_id + "'"#"' AND grade_pointing="+ grade
        #print query
        try:
            cursor = db.cursor()  
            cursor.execute(query)
            db.commit()
            cursor.close()
        except Exception as e:
            print 'Error %s' % e
        finally:
            CMM_DBClose(db)


if __name__ == "__main__":
    # insert_to_ba_db('GW_Initial_S190521r_observation','999','XL001','004','clear','G0014','80000400','80','40','pointing','2019-05-24T00:00:00','2019-05-24T00:30:00','20','test','complete')
    # location = 'xinglong'
    # db = CMM_DBConnect(location)
    # print Retrieve_svom_observatory_telescope_instrument_ID(db,'GWAC_unit4')
    # print Retrieve_svom_observatory_telescope_instrument_ID(db,'GWAC_unit1')
    # print Retrieve_svom_observatory_telescope_instrument_ID(db,'GWAC/F60_B')
    # print Retrieve_svom_observatory_telescope_instrument_ID(db,'GWAC/F30')
    # filter_system = "Johnson"
    # print Retrieve_filter_ID(db,'U',filter_system)
    # CMM_DBClose(db)
    #update_to_ba_db('15270', 'GW_Update_S190521g_observation', 'complete', '2019-05-31 15:28:38')
    # obj = '114758'
    # obj_name = 'G0015_31350127'
    # objsource = 'GW_initial_5987_observation'
    # db = Yunwei_DBConnect('xinglong')
    # sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_name + "'" + 'and objsour=' + "'" + objsource + "'"
    # cur = db.cursor()
    # cur.execute(sql)
    # res = cur.fetchall()
    # if res:
    #     objrank = res[0][0]
    # else:
    #     objrank = '0'
    # if 'G0' in obj_name:
    #     try:
    #         strs = obj_name.split('_')
    #     except:
    #         grid_id = 'G0000'
    #         field_id = obj_name
    #     else:
    #         grid_id = strs[0]
    #         field_id = strs[1]
    # else:
    #     grid_id = 'G0000'
    #     field_id = obj_name
    # print grid_id,field_id,objrank
    # update_pointing_lalalert('5987','F30',grid_id,field_id,objrank,'broken')
    a = 0
