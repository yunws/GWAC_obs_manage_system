#!/usr/bin/env python
"""
Created on Wed Mar  6 03:37:28 2019

@author: Damien
"""

import numpy as np
import stat, sys, os
import math
import gzip
from shutil import copyfile
import csv
import pylab as plt
import argparse
import voeventparse
import json
from astropy.time import Time
import time
import datetime
import pymysql
import psycopg2
import psycopg2.extras

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
    try:
        db=psycopg2.connect("host=" + yunwei_host + " port= " + data["yunwei_port"] + \
        " dbname='" + data["yunwei_db"] + "' user='" + data["yunwei_user"] + "' password='"  + data["yunwei_password"] + "'")
    except:
        print('can not connect to the GWAC DB')
        db = []
    return db

#__________________________________
def Yunwei_DBClose(db):
    "Close the connection to the DB"
    db.close()

#__________________________________
def Retrieve_last_trigger_params(location):
    "Find the ID of the trigger type"
    db = CMM_DBConnect(location)
    if db != []:
        try:
            cursor = db.cursor()
            query = "SELECT ID_external_trigger, external_trigger_name,external_trigger_alert_message_path,external_trigger_alert_message_revision,external_trigger_alert_message_type_name  FROM trigger_v where external_trigger_type_simulation=1  ORDER BY ID_external_trigger DESC LIMIT 1"
            cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                ID_external_trigger = -1
            else:
                result_qry_json = []
                d = {}
                d['ID_external_trigger'] = results[-1][0]
                d['external_trigger_name'] = results[-1][1]
                d['alert_message_path'] = results[-1][2]
                d['alert_message_revision'] = results[-1][3]
                d['alert_message_type'] = results[-1][4]
                result_qry_json = json.dumps(d, ensure_ascii=False)
            cursor.close()
            CMM_DBClose(db)
        except:
            print("1. lost connection to Mysql server during the query")
            d = {}
            d['ID_external_trigger'] = 0
            result_qry_json = json.dumps(d, ensure_ascii=False)
    else:
        d = {}
        d['ID_external_trigger'] = 0
        result_qry_json = json.dumps(d, ensure_ascii=False)
    return result_qry_json

#__________________________________
def Retrieve_newer_trigger_params(location,ID_external_trigger_old):
    "Find the ID of the trigger type"
    db = CMM_DBConnect(location)
    if db != []:
        try:
            cursor = db.cursor()
            query = "SELECT ID_external_trigger,external_trigger_name,external_trigger_time,external_trigger_RA_center," + \
            " external_trigger_dec_center,external_trigger_localisation_error,external_trigger_distance,ID_external_trigger_type, "+ \
            " external_trigger_type_name,ID_source_classification,ID_external_trigger_telescope,external_trigger_alert_message_path, " + \
            " external_trigger_alert_message_revision,external_trigger_alert_message_type_name" +\
            " FROM trigger_v WHERE external_trigger_type_simulation=1  and ID_external_trigger > " + str(ID_external_trigger_old) + " ORDER BY ID_external_trigger "
            cursor.execute(query)
            results = cursor.fetchall()
            # print('results',results)
            if not results:
                result_qry_json = []
                ID_external_trigger = []
                external_trigger_name = []
                external_trigger_time = []
                external_trigger_RA_center = []
                external_trigger_dec_center = []
                external_trigger_localisation_error = []
                external_trigger_distance = []
                ID_external_trigger_type = []
                external_trigger_type_name = []
                ID_source_classification = []
                ID_external_trigger_telescope = []
                alert_message_path = []
                alert_message_revision = []
                alert_message_type = []
            else:
                result_qry_json = []
                ID_external_trigger = []
                external_trigger_name = []
                external_trigger_time = []
                external_trigger_RA_center = []
                external_trigger_dec_center = []  
                external_trigger_localisation_error = [] 
                external_trigger_distance = []             
                ID_external_trigger_type = []
                external_trigger_type_name = []
                ID_source_classification = []
                ID_external_trigger_telescope = []
                alert_message_path = []
                alert_message_revision = []
                alert_message_type = []
                for n in range(len(results)):
                    ID_external_trigger.append(results[n][0])
                    external_trigger_name.append(results[n][1])
                    external_trigger_time.append(results[n][2])
                    external_trigger_RA_center.append(results[n][3])
                    external_trigger_dec_center.append(results[n][4])
                    external_trigger_localisation_error.append(results[n][5])
                    if results[n][6] is None:
                        # print('distance',results[n][12])
                        distance = -1
                    else:
                        distance = results[n][6]
                    external_trigger_distance.append(distance)
                    ID_external_trigger_type.append(results[n][7])
                    external_trigger_type_name.append(results[n][8])
                    ID_source_classification.append(results[n][9])
                    ID_external_trigger_telescope.append(results[n][10])
                    alert_message_path.append(results[n][11])
                    alert_message_revision.append(results[n][12])
                    alert_message_type.append(results[n][13])
            # print(external_trigger_distance)
            d = {}
            d['ID_external_trigger'] = ID_external_trigger
            d['external_trigger_name'] = external_trigger_name
            d['external_trigger_time'] = external_trigger_time
            d['external_trigger_RA_center'] = external_trigger_RA_center
            d['external_trigger_dec_center'] = external_trigger_dec_center
            d['external_trigger_localisation_error'] = external_trigger_localisation_error
            d['external_trigger_distance'] = external_trigger_distance
            d['ID_external_trigger_type'] = ID_external_trigger_type
            d['external_trigger_type_name'] = external_trigger_type_name
            d['ID_source_classification'] = ID_source_classification
            d['ID_external_trigger_telescope'] = ID_external_trigger_telescope
            d['alert_message_path'] = alert_message_path
            d['alert_message_revision'] = alert_message_revision
            d['alert_message_type'] = alert_message_type
            result_qry_json = json.dumps(d, ensure_ascii=False)
            cursor.close()
            CMM_DBClose(db)
        except ValueError as e:
            print(e)
            print("2. lost connection to Mysql server during the query")
            d = {}
            d['ID_external_trigger'] = [0]
            result_qry_json = json.dumps(d, ensure_ascii=False)             
    else:
        d = {}
        d['ID_external_trigger'] = [0]
        result_qry_json = json.dumps(d, ensure_ascii=False) 
    return result_qry_json    

#__________________________________
def Retrieve_svom_observatory_telescope_instrument_ID(db,svom_telescope_name):
    "Find the ID of the svom telescope"
    cursor = db.cursor()
    query = "SELECT * FROM svom_telescope WHERE svom_telescope_name='"+str(svom_telescope_name)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        ID_svom_observatory = -1
        ID_svom_telescope = -1
        ID_svom_instrument = -1
    else:
        ID_svom_telescope = results[-1][0]
        ID_svom_observatory = results[-1][4]
        query = "SELECT * FROM svom_instrument WHERE ID_SVOM_telescope='"+str(ID_svom_telescope)+"'"
        cursor.execute(query)
        result_inst = cursor.fetchall()
        ID_svom_instrument = result_inst[-1][0]
        
    return ID_svom_observatory,ID_svom_telescope, ID_svom_instrument

#__________________________________
def Retrieve_filter_ID(db,band,filter_system):
    "Find the ID of the filter used for the observation"
    cursor = db.cursor()
    if band=='Clear':
        query = "SELECT * FROM filter WHERE filter_name='"+str(band)+"'"
    else:
        query = "SELECT * FROM filter WHERE filter_name='"+str(band)+"/"+str(filter_system)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        ID_filter = -1
    else:
        ID_filter = results[-1][0]
    return ID_filter

#__________________________________
def Retrieve_galaxy_ID(db,galaxy_name):
    "Find the ID of the galaxy observed"
    cursor = db.cursor()
    
    query = "SELECT * FROM galaxy WHERE galaxy_name='"+str(galaxy_name)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        ID_galaxy = -1
    else:
        ID_galaxy = results[-1][0]
    return ID_galaxy

#__________________________________
def Retrieve_lalpointing_list(db,ID_external_trigger):
    "Find the ID of the galaxy observed"
    cursor = db.cursor()
    
    query = "SELECT * FROM pointing_lalalert WHERE ID_external_trigger='"+str(ID_external_trigger)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        results = []
        
    return results

#__________________________________
def Retrieve_pointing_list_lal(location,ID_external_trigger):
    "Retrieve the pointing list from LAL alert table"
    db = CMM_DBConnect(location)
    if db != []:
        try:
            cursor = db.cursor()
            query = "SELECT * FROM pointing_lalalert WHERE ID_external_trigger='"+str(ID_external_trigger)+"'" 
            # print(query)
            cursor.execute(query)
            results = cursor.fetchall()
            # print('results test',results[0][3])
            if not results:
                name_telescope = []
                ID_grid = []
                ID_field = []
                RA_pointing = []
                dec_pointing = []
                grade_pointing = []
            else:
                result_qry_json = []
                name_telescope = []
                ID_grid = []
                ID_field = []
                RA_pointing = []
                dec_pointing = []
                grade_pointing = []
                for n in range(len(results)):
                    name_telescope.append(results[n][2])
                    ID_grid.append(results[n][3])
                    ID_field.append(results[n][4])
                    RA_pointing.append(results[n][5])
                    dec_pointing.append(results[n][6])
                    grade_pointing.append(results[n][7])

            d = {}
            d['name_telescope'] = name_telescope
            d['ID_grid'] = ID_grid
            d['ID_field'] = ID_field
            d['RA_pointing'] = RA_pointing
            d['dec_pointing'] = dec_pointing
            d['grade_pointing'] = grade_pointing
            # print('results',results[-1][7])
            result_qry_json = json.dumps(d, ensure_ascii=False)
            cursor.close()
            CMM_DBClose(db)
        except ValueError as e:
            print(e)
            print("3. lost connection to Mysql server during the query")
            d = {}
            result_qry_json = json.dumps(d, ensure_ascii=False)             
    else:
        d = {}
        result_qry_json = json.dumps(d, ensure_ascii=False) 
    return result_qry_json  

#__________________________________
def Set_reception_status_pointing_list(location,ID_external_trigger):
    "Set reception status of the pointing list from LAL alert table"
    try:
        db = CMM_DBConnect(location)
        cursor = db.cursor()
        query = "UPDATE pointing_lalalert SET pointing_status = 'received @ GWAC' WHERE ID_external_trigger='"+str(ID_external_trigger)+"'" 
        print(query)
        cursor.execute(query)
        db.commit()
        cursor.close()
        CMM_DBClose(db)
    except:
        print("Update reception status failed")            
    return 

#__________________________________
def Insert_galaxy_inDB(db,ID_galaxy,galaxy_name,galaxy_ra,galaxy_dec):
    "INSERT a new galaxy into the CSC DB"
    "-------- Create a folder for this galaxy in the ftp server"
    galaxy_im_path = "galaxy/"+str(galaxy_name)+""
    if os.path.isdir(galaxy_im_path)==False:
        os.mkdir(galaxy_im_path);
        os.chmod(galaxy_im_path, stat.S_IRWXO)
    cursor = db.cursor()
    query = "INSERT INTO `galaxy`(`ID_galaxy`,`galaxy_name`, `galaxy_RA`, `galaxy_dec`, `galaxy_im_path`)"+\
            "VALUES ('"+str(ID_galaxy)+"','"+str(galaxy_name)+"','"+str(galaxy_ra)+"','"+str(galaxy_dec)+"','"+str(galaxy_im_path)+"')"
    cursor.execute(query)


    try:
        db = CMM_DBConnect(location)
        cursor = db.cursor()
        query = "UPDATE pointing_lalalert SET pointing_status = 'received @ GWAC' WHERE ID_external_trigger='"+str(ID_external_trigger)+"'" 
        cursor.execute(query)
        db.commit()
        cursor.close()
        CMM_DBClose(db)
    except:
        print("Update reception status failed") 

#__________________________________
def trigger_type_classification(trigger_type,trigger_time,ID_external_trigger_telescope,alert_message_type):
    "Classify the type of alert"
    if trigger_type == 1:
        if ID_external_trigger_telescope in [1,2]: 
             # print('Fermi GRB trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
            mark1 = 20000           
        if ID_external_trigger_telescope in [3,4,5,6]: 
            # print('SWIFT GRB trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
            mark1 = 30000
        mark5 = 2
    elif trigger_type == 2:
        if ID_external_trigger_telescope in [1,2]: 
             # print('Fermi GRB trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
            mark1 = 20000           
        if ID_external_trigger_telescope in [3,4,5,6]:
            # print('SWIFT GRB trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
            mark1 = 30000
        mark5 = 1
    elif trigger_type == 4:
        # print('GW trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
        mark1 = 10000
        mark5 = 2
    elif trigger_type == 5:
        # print('GW trigger (real):',trigger_name[n],trigger_ID[n],alert_message_type[n])
        mark1 = 10000
        mark5 = 1
    elif trigger_type == 7:
        # print('Neutrino trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
        mark1 = 40000
        mark5 = 1
    elif trigger_type == 8:
        # print('Neutrino trigger (real):',trigger_name[n],trigger_ID[n],alert_message_type[n])
        mark1 = 40000
        mark5 = 2
    else:
        mark1 = 0
        mark5 = 0

    # print(alert_message_type)
    if alert_message_type == 'Initial':
        # print('Initial')
        mark2 = 1000
    elif alert_message_type == 'Update':
        # print('Update')
        mark2 = 2000
    else:
        mark2 = 0          

    utc_time  = time.gmtime()
    utc_time_str = time.strftime('%Y/%m/%d %H:%M:%S', utc_time)
    utc_datetime = datetime.datetime.strptime(utc_time_str, '%Y/%m/%d %H:%M:%S')
    try:
        datet_datetime = datetime.datetime.strptime(trigger_time, '%Y-%m-%d %H:%M:%S')
    except:
        datet_datetime = datetime.datetime.strptime(trigger_time, '%Y-%m-%d %H:%M:%S.%f')
    timedelta = utc_datetime - datet_datetime
    timedelta_seconds = timedelta.total_seconds()
    timedelay_check = timedelta_seconds / 60.0

    if timedelay_check > 3 and timedelay_check <= 30:
        mark4 = 10
    elif timedelay_check > 30:
        mark4 = 20
    elif timedelay_check <= 3:
        mark4 = 30
    else:
        # print('time delay check failed')
        mark4 = 0

    return(mark1,mark2,mark4,mark5) 


#__________________________________
def Telescope_Obs_strategy_match(mark,location):
    "Find the ID of the telescope, observation_strategy, Observation priority"
    match = 0
    if mark in [11111, 11121, 11112, 11122, 11131, 11132, 12111, 12121, 12112, 12122, 12131, 12132]:
        obs_type_id = [12]
        match = 1
    elif mark in [11211, 11221, 11212, 11222, 11231, 11232, 12211, 12221, 12212, 12222, 12231, 12232]:
        obs_type_id = [13,14]  
        match = 1
    elif mark in [21111, 21121, 21112, 21122, 21131, 21132, 22111, 22121, 22112, 22122, 22131, 22132]:
        obs_type_id = [14]  
        match = 1
    elif mark in [21211, 21221, 21212, 21222, 21231, 21232, 22211, 22221, 22212, 22222, 22231, 22232]:
        obs_type_id = [13]  
        match = 1
    elif mark > 30000 and mark < 39999:
        obs_type_id = [16]  
        match = 1
    elif mark > 40000 and mark < 49999:
        obs_type_id = [15]  
        match = 1
    else: 
        obs_type_id = []
        match = 0

    if match == 1:
        db = Yunwei_DBConnect(location)
        if db != []:
            cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

            if len(obs_type_id) == 1:
                query = "SELECT * FROM obs_type_list WHERE id = " + str(obs_type_id[0])
            elif len(obs_type_id) == 2:
                query = "SELECT * FROM obs_type_list WHERE id = " + str(obs_type_id[0]) + " or id = " + str(obs_type_id[1]) + " "

            # query = "SELECT * FROM obs_type_list"
            cursor.execute(query)
            results = cursor.fetchall()
            # print(query,results)
            if not results:
                obs_type_id = []
                obs_type = []
                priority = []
                group_ids = []
                unit_ids = []
            else:
                obs_type_id = []
                obs_type = []
                priority = []
                group_ids = []
                unit_ids = []
                for n in range(len(results)):
                    obs_type_id.append(results[n][0])
                    obs_type.append(results[n][1])
                    priority.append(results[n][3])
                    group_ids.append(results[n][4])
                    unit_ids.append(results[n][5])
                    # print(results[n])

            d = {}
            d['obs_type_id'] = obs_type_id
            d['obs_type'] = obs_type
            d['priority'] = priority
            d['group_ids'] = group_ids
            d['unit_ids'] = unit_ids

            result_qry_json = json.dumps(d, ensure_ascii=False) 
            cursor.close()
            Yunwei_DBClose(db)
        else:
            # print('no observation strategy matched')
            d = {}
            d['obs_type_id'] = [0]
            result_qry_json = json.dumps(d, ensure_ascii=False)
    else:
        # print('no observation strategy matched')
        d = {}
        d['obs_type_id'] = [0]
        result_qry_json = json.dumps(d, ensure_ascii=False)
    return result_qry_json


def insert_trigger_obj_field_op_talbe(location,trigger_ID,Serial_num,Obj_ID,Op_Obj_ID): # 将 trigger ID, 观测天区名, 星系列表记录到数据库
    object_list = "trigger_obj_field_op_sn"
    try:
        db = CMM_DBConnect(location)
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)  
        if (len(Obj_ID) > 0) :
             for n in range(len(Obj_ID)):
                insert_cmd = "insert into " + object_list + " values ( DEFAULT , " + \
                        "'" + trigger_ID + "'," + \
                        "'" + Serial_num + "'," + \
                        "'" + Obj_ID[n] + "'," + \
                        "'" + Op_Obj_ID[n] + "')"    
                # print(insert_cmd)
                cursor.execute(insert_cmd)
                db.commit()
        cursor.close()
        Yunwei_DBClose(db)
    except Exception as e:
        print('Error %s' % e ) 
    return    

def Make_observation_plan(db):
    
    try:
        
        # ask the DB to find the last trigger ID
        trigger_params = json.loads(Retrieve_last_trigger_params(db))
        trigger_name = trigger_params['external_trigger_name']
        trigger_ID = trigger_params['ID_external_trigger']       

        
        # Load the list of pointings from lalalert
        lal_pointings= Retrieve_lalpointing_list(db,trigger_ID)
        
        #----- Retrieve the name of the telescope for which the obs plan is made
        telescope_name = lal_pointings[0][2] #normally GWAC, SVOM/F60, SVOM/F30, C-GFT
        #----- Retrieve the grid_ID corresponding to the observation plan
        ID_grid = lal_pointings[0][3]
        # Retrieve the pointing list from LAL alert
        ID_field = []
        RA_list = []
        dec_list = []
        grade_list = []
        ID_galaxy = []
        galaxy_name = []
        for i in range(len(lal_pointings)-1):
            ID_field.append(lal_pointings[i][4])
            RA_list.append(lal_pointings[i][5])
            dec_list.append(lal_pointings[i][6])
            grade_list.append(lal_pointings[i][7])
            if telescope_name == 'SVOM/F60':
                # ----- If galaxy targeting fill the galaxy table for the unknown galaxies
                if Retrieve_galaxy_ID(db,ID_field[i])==-1:
# =============================================================================
#  #--- A code here to retrieve the real galaxy name in catalogs ? Otherwise galaxy_name = galaxy_ID
# =============================================================================
                    if not galaxy_name:
                        Insert_galaxy_inDB(db,ID_field[i],ID_field[i],lal_pointings[i][5],lal_pointings[i][6])
                   
                    else:
                        Insert_galaxy_inDB(db,ID_field[i],galaxy_name[i],lal_pointings[i][5],lal_pointings[i][6])
# =============================================================================
#   Here you are going to make the obs plan
# =============================================================================

#   The obs plan should provide to the CMM DB the following informations (for a given trigger) 
#   once it is processed
#  1/ The Grid ID : provided by the DB above
#  2/ list of Field IDs : provided by the DB above
#  3/ list of RA : provided by the DB above
#  4/ list of dec : provided by the DB above
#  5/ list of obs strategy type (tiling or pointing) : provided by the GWAC's code
#  6/ list of Tstart : provided by the GWAC's code
#  7/ list of Tend : provided by the GWAC's code
#  8/ list of filter IDs : use ID_filter = Retrieve_filter_ID(db,band,filter_system)
#  9/ list of observatory,telescope, instru IDs : use telescope_IDs =  Retrieve_svom_observatory_telescope_instrument_ID(db,telescope_name)
# Beware the telescope name in the DB are listed with the following names
# for GWAC : GWAC_unitX, for GWAC-F60 : GWAC/F60_A or GWAC/F60_B for GWAC_F30 : GWAC/F30, for the C-GFT : SVOM_CGFT

#  10/ list of galaxy IDs : use ID_galaxy = Retrieve_galaxy_ID(db,galaxy_name):
#  11/ list of contact name and mail : provided by the GWAC's code
#  12/ the typical exposure time (in sec) used for each pointing : provided by the GWAC's code
#  13/ the observation type (tiling or pointing) : provided by the GWAC's code
    except Exception as e:
        now = str(Time.now())
        log.write(now[0:22]+":"+delimiter_type+" An ERROR occured when trying to make the observation_plan  \n")
        now = str(Time.now())
        log.write(now[0:22]+":"+delimiter_type+str(e)+"  \n")
        now = str(Time.now())
        log.write(now+":"+delimiter_type+" The observation plan has not been performed \n ")
        now = str(Time.now())
        log.write(now[0:22]+":"+delimiter_type+" End of the observation plan code \n==========\n")
# =============================================================================
# Once the obs plan is generated you can fill the DB there
# =============================================================================
    
#    try:
#        for i in range(len(Grid_ID_planned)):
#            query = "INSERT INTO `observation_plan`(`ID_external_trigger`,`ID_SVOM_observatory`"+\
#                    "`ID_SVOM_telescope`, `ID_SVOM_instrument`,`ID_filter`,`ID_grid`, `ID_field`,"+\
#                    "`RA_center`, `dec_center`, `observation_type`, `T_start`,`T_end`, `exposure`,"+\
#                    "`ID_galaxy`, `duty_scientist_name`, `duty_scientist_mail`) "+\
#                    "VALUES ('"+str(trigger_ID)+"','"+str(Observatory_ID[i])+"','"+str(Telescope_ID_planned[i])+"','"+str(Instrument_ID_planned[i])+"',"+\
#                    "'"+str(ID_filter_planned[i])+"','"+str(Grid_ID_planned[i])+"','"+str(Field_ID_planned[i])+"',"+\
#                    "'"+str(RA_planned[i])+"','"+str(dec_planned[i])+"','"+str(obs_strategy_planned[i])+"',"+\
#                    "'"+str(Tstart_planned[i])+"','"+str(Tend_planned[i])+"','"+str(exposure_planned[i])+"',"+\
#                    "'"+str(galaxy_ID_planned[i])+"','"+str(contact_name_planned[i])+"','"+str(contact_mail_planned[i])+"')"
##            cursor.execute(query)
#    except Exception as e:
#        now = str(Time.now())
#        log.write(now[0:22]+":"+delimiter_type+" An ERROR occured when trying to fill the DB : observation_plan table  \n")
#        now = str(Time.now())
#        log.write(now[0:22]+":"+delimiter_type+str(e)+"  \n")
#        now = str(Time.now())
#        log.write(now+":"+delimiter_type+" The observation plan has not been stored in the DB \n ")
#        now = str(Time.now())
#        log.write(now[0:22]+":"+delimiter_type+" End of the observation plan code \n==========\n")

    return RA_list

#__________________________________
if __name__ == "__main__":
    
    #Write logs
    now = str(Time.now())
    #----- Check if the year folder is created
    data_path = './logs/'+now[0:4]
    if os.path.isdir(data_path)==False:
        os.mkdir(data_path);
        os.chmod(data_path, stat.S_IRWXO)
    #----- Check if the month folder is created
    data_path = './logs/'+now[0:4]+'/'+now[5:7]
    if os.path.isdir(data_path)==False:
        os.mkdir(data_path);
        os.chmod(data_path, stat.S_IRWXO)
    
    data_file = 'logs_'+now[0:10]+'.txt'
    log_path = data_path+'/'+data_file
    if os.path.isfile(log_path)==False:
        delimiter_type = "\t"
        log = open(log_path, 'w')
        log.write("================================================================================== \n")
        log.write("                               The SVOM BA system \n")
        log.write("================================================================================== \n")
        log.write("         Logs for the date : "+now[0:10]+" generated automatically \n")
        log.write("\n\n")
    else:
        delimiter_type = "\t"
        log = open(log_path, 'a')
    
    
    db = DBConnect()
    
    now = str(Time.now())
    log.write(now[0:22]+":"+delimiter_type+" Starting the observation plan code  \n")
    
#    obsplan = Make_observation_plan(db)
    
    log.close()
    
#    # testing lines
    ID_external_trigger_old = 993
    last_trigger_params = Retrieve_last_trigger_params(db)
    print(last_trigger_params)
    svom_tel_IDsinDB = Retrieve_svom_observatory_telescope_instrument_ID(db,'GWAC_unit4')
    filter_IDinDB = Retrieve_filter_ID(db,'R','Johnson')
    trigger_params = json.loads(last_trigger_params)
    ID_external_trigger = trigger_params['ID_external_trigger']    
    lal_pointinglist= Retrieve_lalpointing_list(db,ID_external_trigger)
#    Insert_galaxy_inDB(db,'PGC454','150','20')
#    e = Retrieve_galaxy_ID(db,'1025')
    
    DBClose(db)

