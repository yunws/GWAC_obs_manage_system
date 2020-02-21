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
import pymysql
import time
import datetime
import psycopg2
import psycopg2.extras

#__________________________________
def CMM_DBConnect():
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    db = pymysql.connect(data["CMM_host"],data["CMM_user"],data["CMM_password"],data["CMM_db"] )
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

#__________________________________
def Retrieve_last_trigger_params():
    "Find the ID of the trigger type"
    db = CMM_DBConnect()
    cursor = db.cursor()
    query = "SELECT * FROM trigger_v ORDER BY ID_external_trigger DESC LIMIT 1"
    cursor.execute(query)
    results = cursor.fetchall()
    # print('results',results)
    if not results:
        ID_external_trigger = -1
    else:
        result_qry_json = []
        d = {}
        d['ID_external_trigger'] = results[-1][0]
        d['external_trigger_name'] = results[-1][2]
        d['external_trigger_time'] = results[-1][5]
        d['ID_external_trigger_type'] = results[-1][18]
        d['external_trigger_type_name'] = results[-1][19]
        d['alert_message_path'] = results[-1][46]
        d['alert_message_revision'] = results[-1][47]
        d['alert_message_type'] = results[-1][48]
        result_qry_json = json.dumps(d, ensure_ascii=False)
    CMM_DBClose(db)
    return result_qry_json

#__________________________________
def Retrieve_newer_trigger_params(ID_external_trigger_old):
    "Find the ID of the trigger type"
    db = CMM_DBConnect()
    cursor = db.cursor()
    query = "SELECT * FROM trigger_v WHERE ID_external_trigger > " + str(ID_external_trigger_old) + " ORDER BY ID_external_trigger "
    cursor.execute(query)
    results = cursor.fetchall()
    # print('results',results)
    if not results:
        result_qry_json = []
        ID_external_trigger = []
        external_trigger_name = []
        external_trigger_time = []
        ID_external_trigger_type = []
        external_trigger_type_name = []
        alert_message_path = []
        alert_message_revision = []
        alert_message_type = []
    else:
        result_qry_json = []
        ID_external_trigger = []
        external_trigger_name = []
        external_trigger_time = []
        ID_external_trigger_type = []
        external_trigger_type_name = []
        alert_message_path = []
        alert_message_revision = []
        alert_message_type = []
        for n in range(len(results)):
            ID_external_trigger.append(results[n][0])
            external_trigger_name.append(results[n][2])
            external_trigger_time.append(results[n][5])
            ID_external_trigger_type.append(results[n][18])
            external_trigger_type_name.append(results[n][19])
            alert_message_path.append(results[n][46])
            alert_message_revision.append(results[n][47])
            alert_message_type.append(results[n][48])

    d = {}
    d['ID_external_trigger'] = ID_external_trigger
    d['external_trigger_name'] = external_trigger_name
    d['external_trigger_time'] = external_trigger_time
    d['ID_external_trigger_type'] = ID_external_trigger_type
    d['external_trigger_type_name'] = external_trigger_type_name
    d['alert_message_path'] = alert_message_path
    d['alert_message_revision'] = alert_message_revision
    d['alert_message_type'] = alert_message_type
    result_qry_json = json.dumps(d, ensure_ascii=False)
    CMM_DBClose(db)
    return result_qry_json    

#__________________________________
def Retrieve_svom_telescope_instrument_ID(svom_telescope_name):
    "Find the ID of the svom telescope"
    db = CMM_DBConnect()
    # print("Find the ID of the svom telescope")
    cursor = db.cursor()
    query = "SELECT * FROM svom_telescope WHERE svom_telescope_name='"+str(svom_telescope_name)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    # print('results',results)
    if not results:
        ID_svom_telescope = -1
        ID_svom_instrument = -1
    else:
        ID_svom_telescope = results[-1][0]
        ID_svom_telescope = 4
        query = "SELECT * FROM svom_instrument WHERE ID_SVOM_telescope='"+str(ID_svom_telescope)+"'"
        cursor.execute(query)
        result_inst = cursor.fetchall()
        ID_svom_instrument = result_inst[-1][0]
    # print(ID_svom_telescope, ID_svom_instrument)
    CMM_DBClose(db)
    return ID_svom_telescope, ID_svom_instrument

#__________________________________
def Retrieve_pointing_list(ID_external_trigger):
    "Retrieve the pointing list from LAL alert table"
    db = CMM_DBConnect()
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
    CMM_DBClose(db)
    return result_qry_json        


#__________________________________
def Retrieve_filter_ID(band,filter_system):
    "Find the ID of the filter used for the observation"
    db = CMM_DBConnect()
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
    CMM_DBClose(db)
    return ID_filter

#__________________________________
def Retrieve_galaxy_ID(galaxy_name):
    "Find the ID of the galaxy observed"
    db = CMM_DBConnect()
    cursor = db.cursor()
    
    query = "SELECT * FROM galaxy WHERE galaxy_name='"+str(galaxy_name)+"'"
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        ID_galaxy = -1
    else:
        ID_galaxy = results[-1][0]
    CMM_DBClose(db)
    return ID_galaxy

#__________________________________
def Insert_galaxy_inDB(galaxy_name,galaxy_ra,galaxy_dec):
    "INSERT a new galaxy into the CSC DB"
    "-------- Create a folder for this galaxy in the ftp server"
    db = CMM_DBConnect()
    galaxy_im_path = "galaxy/"+str(galaxy_name)+""
    if os.path.isdir(galaxy_im_path)==False:
        os.mkdir(galaxy_im_path);
        os.chmod(galaxy_im_path, stat.S_IRWXO)
    cursor = db.cursor()
    query = "INSERT INTO `galaxy`(`galaxy_name`, `galaxy_RA`, `galaxy_dec`, `galaxy_im_path`)"+\
            "VALUES ('"+str(galaxy_name)+"','"+str(galaxy_ra)+"','"+str(galaxy_dec)+"','"+str(galaxy_im_path)+"')"
    cursor.execute(query)
    CMM_DBClose(db)

#__________________________________
def Telescope_Obs_strategy_match(mark,location):
    "Find the ID of the telescope, observation_strategy, Observation priority"
    if mark in [11111,11121,12111,12121,11112,11122,12112,12122]:
        obs_type_id = [12]
    elif mark in [11211,11221,12211,12221,11212,11222,12212,12222]:
        obs_type_id = [13,14]  
    db = Yunwei_DBConnect(location)
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if len(obs_type_id) == 1:
        query = "SELECT * FROM obs_type_list WHERE id = 12 "
    elif len(obs_type_id) == 2:
        query = "SELECT * FROM obs_type_list WHERE id =  13 or id = 14 "
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
    return result_qry_json
    
    
# =============================================================================
# Function used to decode the VOEvent sent by LAL
# =============================================================================
def VOEvent_decoder(XML_file):
    
    result_qry_json = []
    with open(XML_file, 'rb') as f:
        v = voeventparse.load(f)

    # Basic attribute access
    Ivorn = v.attrib['ivorn']
    VOE_role = v.attrib['role']
    AuthorIVORN = v.Who.AuthorIVORN
    VOE_date = v.Who.Date
    Name_sender = v.Who.Author.shortName
    Phone_sender = v.Who.Author.contactPhone
    Mail_sender = v.Who.Author.contactEmail
    
    # Copying by value, and validation:
    #print("Original valid as v2.0? ", voeventparse.valid_as_v2_0(v))
    #v_copy = copy.copy(v)
    #print("Copy valid? ", voeventparse.valid_as_v2_0(v_copy))
    
    
    #######################################################
    # And now, parse the VOEvent
    #######################################################
    #c = voeventparse.get_event_position(v)
    #print("Coords:", c)
    
    # =============================================================================
    #  Retrieve the WHAT Params
    # =============================================================================
    
    toplevel_params = voeventparse.get_toplevel_params(v)
    #print("Params:", toplevel_params)
    #print()
    for par in toplevel_params:
        Event_ID = toplevel_params['Event_ID']['value'];
        Event_type = toplevel_params['Event_type']['value'];
        Event_inst = toplevel_params['Event_inst']['value'];
        Loc_url = toplevel_params['Loc_url']['value'];
        BA_name = toplevel_params['FA']['value'];
        Prob = toplevel_params['Prob']['value'];
        Quicklook_url = toplevel_params['Quicklook_url']['value'];
        Distance = toplevel_params['Distance']['value'];
        Err_distance = toplevel_params['Err_distance']['value'];
        fifty_cr_skymap = toplevel_params['50cr_skymap']['value'];
        ninety_cr_skymap = toplevel_params['90cr_skymap']['value'];
        FAR = toplevel_params['FAR']['value'];
        Group = toplevel_params['Group']['value'];
        Pipeline = toplevel_params['Pipeline']['value'];
        Obs_req = toplevel_params['Obs_req']['value'];
        
    grouped_params = voeventparse.get_grouped_params(v)
    #print("Group Params:", grouped_params)
    #print()
    
    for par in grouped_params:
        Event_status = grouped_params['Status']['Event_status']['value'];
        Revision = grouped_params['Status']['Revision']['value']
        Prob_BNS = grouped_params['Classification']['BNS']['value'];
        Prob_NSBH = grouped_params['Classification']['NSBH']['value'];
        Prob_BBH = grouped_params['Classification']['BBH']['value'];
        Prob_Terrestrial = grouped_params['Classification']['Terrestrial']['value'];
        Prob_NS = grouped_params['Properties']['HasNS']['value'];
        Prob_EM = grouped_params['Properties']['HasRemnant']['value'];
        Name_svom_tel = grouped_params['Set_up_OS']['Name_tel']['value'];
        FOV_svom_tel = grouped_params['Set_up_OS']['FOV']['value'];
        FOV_coverage_svom_tel = grouped_params['Set_up_OS']['FOV_coverage']['value'];
        Mag_limit_svom_tel = grouped_params['Set_up_OS']['Mag_limit']['value'];
        exposure_svom_tel = grouped_params['Set_up_OS']['exposure']['value'];
        Slew_rate_svom_tel = grouped_params['Set_up_OS']['Slew_rate']['value'];
        Readout_svom_tel = grouped_params['Set_up_OS']['Readout']['value'];
        Filters_svom_tel = grouped_params['Set_up_OS']['Filters_tel']['value'];
        Latitude_svom_tel = grouped_params['Set_up_OS']['Latitude']['value'];
        Longitude_svom_tel = grouped_params['Set_up_OS']['Longitude']['value'];
        Elevation_svom_tel = grouped_params['Set_up_OS']['Elevation']['value'];
        
        
    obs_plan_RA_unit = v.What.Table.Field[1].attrib['unit']
    obs_plan_dec_unit = v.What.Table.Field[2].attrib['unit']
    
    obs_plan_Grid_ID = []
    obs_plan_RA_center = []
    obs_plan_dec_center = []
    obs_plan_OS_grade = []
    for par in v.What.Table.Data.TR:
        obs_plan_Grid_ID.append(par.TD[0])
        obs_plan_RA_center.append(par.TD[1])
        obs_plan_dec_center.append(par.TD[2])
        obs_plan_OS_grade.append(par.TD[3])
        
    # =============================================================================
    # Retrieve the WHERE & WHEN Params
    # =============================================================================
    
    trigger_Collab = v.WhereWhen.ObsDataLocation.ObservatoryLocation.attrib['id']
    AstroCoordSystem = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoordSystem.attrib['id']
    Time_unit = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Time.attrib['unit']
    Trigger_date = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Time.TimeInstant.ISOTime
    Trigger_date_jd = Time(Trigger_date+'.00')
    Trigger_date_jd_start  = Trigger_date_jd.jd
    Trigger_date = str(Trigger_date).replace('T',' ')
    Trigger_pos_unit = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.attrib['unit']
    Trigger_RA = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C1
    Trigger_dec = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C2
    Trigger_poserr = v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Error2Radius
    
    # =============================================================================
    # Retrieve the WHY Params
    # =============================================================================
    
    alert_importance = v.Why.attrib['importance']
    
    # =============================================================================
    # First retrieve some params in the DB to fill the trigger table
    # =============================================================================
    ID_SVOM_ba_shift = Retrieve_BA_ID(db,Trigger_date_jd_start)
    
    ID_external_trigger_type = Retrieve_trigger_type_ID(db,Event_type,VOE_role)
    ID_external_trigger_telescope = Retrieve_telescope_type_ID(db,Event_inst)

    
    result_qry_json = []
    d = {}
    d["Alert_type"] = VOE_role
    d["Event_ID"] = Event_ID
    d["Event_type"] = Event_type
    d["Event_inst"] = Event_inst
    d["Loc_url"] = Loc_url
    d["BA_name"] = BA_name
    d["Prob"] = Prob
    d["Quicklook_url"] = Quicklook_url
    d["Distance"] = Distance
    d["Err_distance"] = Err_distance
    d["fifty_cr_skymap"] = fifty_cr_skymap
    d["ninety_cr_skymap"] = ninety_cr_skymap
    d["FAR"] = FAR
    d["Group"] = Group
    d["Pipeline"] = Pipeline
    d["Obs_req"] = Obs_req
    d["Event_status"] = Event_status
    d["Revision"] = Revision
    d["Prob_BNS"] = Prob_BNS
    d["Prob_NSBH"] = Prob_NSBH
    d["Prob_BBH"] = Prob_BBH
    d["Prob_Terrestrial"] = Prob_Terrestrial
    d["Prob_NS"] = Prob_NS
    d["Prob_EM"] = Prob_EM
    d["Name_svom_tel"] = Name_svom_tel
    d["FOV_svom_tel"] = FOV_svom_tel
    d["FOV_coverage_svom_tel"] = FOV_coverage_svom_tel
    d["Mag_limit_svom_tel"] = Mag_limit_svom_tel
    d["exposure_svom_tel"] = exposure_svom_tel
    d["Slew_rate_svom_tel"] = Slew_rate_svom_tel
    d["Readout_svom_tel"] = Readout_svom_tel
    d["Filters_svom_tel"] = Filters_svom_tel
    d["Latitude_svom_tel"] = Latitude_svom_tel
    d["Longitude_svom_tel"] = Longitude_svom_tel
    d["Elevation_svom_tel"] = Elevation_svom_tel
    d["Longitude_svom_tel"] = Longitude_svom_tel
    d["obs_plan_RA_unit"] = obs_plan_RA_unit
    d["obs_plan_dec_unit"] = obs_plan_dec_unit
    d["obs_plan_Grid_ID"] = obs_plan_Grid_ID
    d["obs_plan_RA_center"] = obs_plan_RA_center
    d["obs_plan_dec_center"] = obs_plan_dec_center
    d["obs_plan_OS_grade"] = obs_plan_OS_grade
    d["trigger_Collab"] = trigger_Collab
    d["AstroCoordSystem"] = AstroCoordSystem
    d["Time_unit"] = Time_unit
    d["Trigger_date"] = str(Trigger_date)
    d["Trigger_date_jd"] = str(Trigger_date_jd_start)
    d["Trigger_pos_unit"] = Trigger_pos_unit
    d["Trigger_RA"] = str(Trigger_RA)
    d["Trigger_dec"] = str(Trigger_dec)
    d["Trigger_poserr"] = str(Trigger_poserr)
    d["alert_importance"] = str(alert_importance)
    d["ID_SVOM_ba_shift"] = ID_SVOM_ba_shift
    d["ID_external_trigger_type"] = ID_external_trigger_type
    d["ID_external_trigger_telescope"] = ID_external_trigger_telescope
    
    result_qry_json = json.dumps(d, ensure_ascii=False)
        
       
    return result_qry_json,obs_plan_RA_unit,obs_plan_dec_unit,obs_plan_Grid_ID,obs_plan_RA_center,obs_plan_dec_center,obs_plan_OS_grade

def Make_observation_plan():

    # #----- Retrieve the pointing list sent from LAL
    # ID_grid = []
    # ID_field = []
    # RA_pointing = []
    # dec_pointing = []
    # grade_pointing = []
    # trigger_ID = 35
    # telescope_name = 'SVOM/F60'
    # pointing_list_params = json.loads(Retrieve_pointing_list(db,trigger_ID))
    # ID_grid = pointing_list_params['ID_grid']
    # ID_field = pointing_list_params['ID_field']
    # RA_pointing = pointing_list_params['RA_pointing']
    # dec_pointing = pointing_list_params['dec_pointing']
    # grade_pointing = pointing_list_params['grade_pointing']
    # print('pointing list',ID_grid,ID_field,RA_pointing,dec_pointing,grade_pointing)

    # # ask the DB to find the last alert and VOEvent XML file
    # trigger_params = json.loads(Retrieve_last_trigger_params(db))
    # # print(trigger_params)
    # trigger_name = trigger_params['external_trigger_name']
    # trigger_ID = trigger_params['ID_external_trigger']   
    # trigger_type = trigger_params['ID_external_trigger_type']     
    # # XML_file_path = "../"+str(trigger_params['alert_message_path'])+"/"+\
    # #                 ""+str(trigger_params['alert_message_type'])+str(trigger_params['alert_message_revision'])+".xml" 
    # trigger_real = 'test'
    # if trigger_type == 4:
    #     print('GW trigger (test):',trigger_name,trigger_ID,trigger_type)
    #     trigger_real = 'test'
    # elif trigger_type == 5:
    #     print('GW trigger (real):',trigger_name,trigger_ID,trigger_type)
    #     trigger_real = 'real'


    try:
        # ask the DB to find the last alert and VOEvent XML file
        trigger_params = json.loads(Retrieve_last_trigger_params(db))
        # print(trigger_params)
        trigger_name = trigger_params['external_trigger_name']
        trigger_time = trigger_params['external_trigger_time']
        trigger_ID = trigger_params['ID_external_trigger']   
        trigger_type = trigger_params['ID_external_trigger_type']     
        alert_message_type = trigger_params['alert_message_type']
        # XML_file_path = "../"+str(trigger_params['alert_message_path'])+"/"+\
        #                 ""+str(trigger_params['alert_message_type'])+str(trigger_params['alert_message_revision'])+".xml" 

        # #------ Load the json VOE data
        # VOE_decoder_res = VOEvent_decoder(XML_file_path)
        # a = json.loads(VOE_decoder_res[0])    
        
        #----- Retrieve the name of the telescope for which the obs plan is made
        telescope_name = 'GWAC_unit2'
        telescope_IDs = Retrieve_svom_telescope_instrument_ID(db,telescope_name)
        print('telescope',telescope_name,telescope_IDs)

        #----- Retrieve the pointing list sent from LAL
        name_telescope = []
        ID_grid = []
        ID_field = []
        RA_pointing = []
        dec_pointing = []
        grade_pointing = []
        trigger_ID = 35
        pointing_list_params = json.loads(Retrieve_pointing_list(db,trigger_ID))
        name_telescope = pointing_list_params['name_telescope']
        ID_grid = pointing_list_params['ID_grid']
        ID_field = pointing_list_params['ID_field']
        RA_pointing = pointing_list_params['RA_pointing']
        dec_pointing = pointing_list_params['dec_pointing']
        grade_pointing = pointing_list_params['grade_pointing']
        # print('pointing list',name_telescope,ID_grid,ID_field,RA_pointing,dec_pointing,grade_pointing)

        if trigger_type == 4:
            print('GW trigger (test):',trigger_name,trigger_ID,alert_message_type)
            mark1 = 10000
            mark5 = 1
        elif trigger_type == 5:
            print('GW trigger (real):',trigger_name,trigger_ID,alert_message_type)
            mark1 = 10000
            mark5 = 2

        if alert_message_type == 'Initial':
            print('Initial')
            mark2 = 1000
        if alert_message_type == 'Update':
            print('Update')
            mark2 = 2000
        
        if 'SVOM/GWAC' in name_telescope:
            print('tiles')
            mark3 = 100
        if 'SVOM/F60' in name_telescope:
            print('galaxies')
            mark3 = 200



        utc_time  = time.gmtime()
        utc_time_str = time.strftime('%Y/%m/%d %H:%M:%S', utc_time)
        utc_datetime = datetime.datetime.strptime(utc_time_str, '%Y/%m/%d %H:%M:%S')
        datet_datetime = datetime.datetime.strptime(trigger_time, '%Y-%m-%d %H:%M:%S.%f')
        timedelta = utc_datetime - datet_datetime
        timedelta_seconds = timedelta.total_seconds()
        timedelay_check = timedelta_seconds / 60.0

        print(timedelay_check)
        if timedelay_check <= 30:
            mark4 = 10
        elif timedelay_check > 30:
            mark4 = 20
        else:
            print('time delay check failed')

        mark = mark1 + mark2 + mark3 + mark4 + mark5
        if mark == 11111:
            print('it is a GW trigger: initial, tiles, early, real')
        elif mark == 11121:
            print('it is a GW trigger: initial, tiles, late, real')
        elif mark == 12111:
            print('it is a GW trigger: Update, tiles, early, real')
        elif mark == 12121:
            print('it is a GW trigger: Update, tiles, late, real')

        elif mark == 11112:
            print('it is a GW trigger: initial, tiles, early, test')
        elif mark == 11122:
            print('it is a GW trigger: initial, tiles, late, test')
        elif mark == 12112:
            print('it is a GW trigger: Update, tiles, early, test')
        elif mark == 12122:
            print('it is a GW trigger: Update, tiles, late, test')

        elif mark == 11211:
            print('it is a GW trigger: initial, galaxies, early, real')
        elif mark == 11221:
            print('it is a GW trigger: initial, galaxies, late, real')
        elif mark == 12211:
            print('it is a GW trigger: Update, galaxies, early, real')
        elif mark == 12221:
            print('it is a GW trigger: Update, galaxies, late, real')

        elif mark == 11212:
            print('it is a GW trigger: initial, galaxies, early, test')
        elif mark == 11222:
            print('it is a GW trigger: initial, galaxies, late, test')
        elif mark == 12212:
            print('it is a GW trigger: Update, galaxies, early, test')
        elif mark == 12222:
            print('it is a GW trigger: Update, galaxies, late, test')

        # Retrieve the list of Grid ID
        Grid_ID_list = VOE_decoder_res[3]
        # Retrieve the list of the field ID (Not available yet)
        
        # Retrieve the galaxy name (Not available yet)
        
        ID_galaxy = Retrieve_galaxy_ID(db,galaxy_name)
        # Retrieve the list of RA pointings
        RA_list = VOE_decoder_res[4]
        # Retrieve the list of dec pointings
        dec_list = VOE_decoder_res[5]
        # Retrieve the list of grades for each pointing
        grade_list = VOE_decoder_res[6]
        
        # ----- If galaxy targeting fill the galaxy table for the unknown galaxies
        if ID_galaxy==-1:
            Insert_galaxy_inDB(db,galaxy_name,galaxy_ra,galaxy_dec)
            ID_galaxy = Retrieve_galaxy_ID(db,galaxy_name)
    
    
    
# =============================================================================
#   Here you are going to make the obs plan
# =============================================================================

#   The obs plan should provide the following informations (for a given trigger) 
#   once it is processed
#  1/ list of Grid IDs : provided by the LAL VOE
#  2/ list of Field IDs : provided by the LAL VOE
#  3/ list of RA : provided by the LAL VOE
#  4/ list of dec : provided by the LAL VOE
#  5/ list of obs strategy type (tiling or pointing) : provided here 
#  6/ list of Tstart : provided here
#  7/ list of Tend : provided here
#  8/ list of filter IDs : use ID_filter = Retrieve_filter_ID(db,band,filter_system)
#  9/ list of telescope IDs : use telescope_IDs = Retrieve_svom_telescope_instrument_ID(db,svom_telescope_name)
#  10/ list of instrument IDs : use telescope_IDs = Retrieve_svom_telescope_instrument_ID(db,svom_telescope_name)
#  11/ list of galaxy IDs : use ID_galaxy = Retrieve_galaxy_ID(db,galaxy_name):
#  8/ list of contact name and mail : provided here
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
    
    try:
        for i in range(len(Grid_ID_planned)):
            query = "INSERT INTO `observation_plan`(`ID_external_trigger`,"+\
                    "`ID_grid`, `ID_field`, `RA_center`, `dec_center`, `observation_type`, `T_start`,"+\
                    "`T_end`, `ID_filter`, `exposure`, `ID_SVOM_telescope`, `ID_SVOM_instrument`, "+\
                    "`ID_galaxy`, `duty_scientist_name`, `duty_scientist_mail`) "+\
                    "VALUES ('"+str(trigger_ID)+"','"+str(Grid_ID_planned[i])+"','"+str(Field_ID_planned[i])+"',"+\
                    "'"+str(RA_planned[i])+"','"+str(dec_planned[i])+"','"+str(obs_strategy_planned[i])+"',"+\
                    "'"+str(Tstart_planned[i])+"','"+str(Tend_planned[i])+"','"+str(ID_filter_planned[i])+"','"+str(exposure_planned[i])+"',"+\
                    "'"+str(Telescope_ID_planned[i])+"','"+str(Instrument_ID_planned[i])+"','"+str(galaxy_ID_planned[i])+"',"+\
                    "'"+str(contact_name_planned[i])+"','"+str(contact_mail_planned[i])+"')"
            cursor.execute(query)
    except Exception as e:
        now = str(Time.now())
        log.write(now[0:22]+":"+delimiter_type+" An ERROR occured when trying to fill the DB : observation_plan table  \n")
        now = str(Time.now())
        log.write(now[0:22]+":"+delimiter_type+str(e)+"  \n")
        now = str(Time.now())
        log.write(now+":"+delimiter_type+" The observation plan has not been stored in the DB \n ")
        now = str(Time.now())
        log.write(now[0:22]+":"+delimiter_type+" End of the observation plan code \n==========\n")

#__________________________________
if __name__ == "__main__":
    
    #Write logs
    now = str(Time.now())
    print(now)
    #----- Check if the year folder is created
    # data_path = '../../im/logs/'+now[0:4]+'/'
    data_path = './logs/'+now[0:4]+'/'
    if os.path.isdir(data_path)==False:
        os.mkdir(data_path);
        os.chmod(data_path, stat.S_IRWXO)
    #----- Check if the month folder is created
    data_path = './logs/'+now[0:4]+'/'+now[5:7]+'/'
    if os.path.isdir(data_path)==False:
        os.mkdir(data_path);
        os.chmod(data_path, stat.S_IRWXO)
    
    data_file = 'logs_'+now[0:10]+'.txt'
    log_path = data_path+data_file
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
    
    
    # db = CMM_DBConnect()
    
    ID_external_trigger_old = 400
    trigger_params = json.loads(Retrieve_newer_trigger_params(ID_external_trigger_old))
    print(trigger_params)


    now = str(Time.now())
    log.write(now[0:22]+":"+delimiter_type+" Starting the observation plan code  \n")
    
    Make_observation_plan()
    
    log.close()
    
#    # testing lines
#    a = Retrieve_last_trigger_params(db)
    b = Retrieve_svom_telescope_instrument_ID('GWAC_unit4')
    print(b)
#    c = Retrieve_filter_ID(db,'R','Johnson')
#    Insert_galaxy_inDB(db,'PGC454','150','20')
    
    # CMM_DBClose(db)



    