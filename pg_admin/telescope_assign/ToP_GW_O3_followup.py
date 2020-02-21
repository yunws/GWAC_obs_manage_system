"""

   Pubsub envelope subscriber   
 
   Author: Xuhui Han
  
"""
# from version import version
# import zmq
# import sys
# from jdcal import gcal2jd
# import os
# import datetime
# import time
# import MySQLdb
# import socket
# from search_box import search_box
# from optparse import OptionParser
# from GWAC_neutrino_followup import GWAC_neutrino_followup
# from GWAC_GW_followup import GWAC_GW_followup
# from GWAC_GBM_gwacfollowup import GWAC_GBM_gwacfollowup
# from func_GRBalerts_slack import func_GRBalerts_slack
# from func_slack_message import func_slack_message
# from func_GBM_111_observable_check import func_GBM_111_observable_check

# # from func_slack_test import func_GRBalerts_slack

# tele60_followup_codepath = os.getcwd() + '/tele60_followup/'
# sys.path.insert(0, tele60_followup_codepath)
# from tele60_GRB_followup import tele60_GRB_followup
# from tele60_GBM_followup import tele60_GBM_followup

# import threading

import sys
import time
import datetime
import json
from obsplan_to_DB import Retrieve_last_trigger_params
from obsplan_to_DB import Retrieve_newer_trigger_params
from obsplan_to_DB import Retrieve_pointing_list
from obsplan_to_DB import Telescope_Obs_strategy_match
from ToP_GW_followup_GWAC_tile_assign import ToP_GW_followup_GWAC_tile_assign
from ToP_GW_followup_F60_F30_galaxy_assign import ToP_GW_followup_F60_F30_galaxy_assign

# # set mode before impliment
# # mode = 'observation'
# mode = 'test'
# # set location
# location = 'beijing'
# # location = 'xinglong'

if __name__ == '__main__':
#    print sys.argv  
    z=0
if not sys.argv[1:]:
    sys.argv += ["test", "beijing", "./"]

mode = sys.argv[1]
location = sys.argv[2]
homedir = sys.argv[3]


if mode == 'test':

    currenttime_time = time.strptime('2018-03-12 19:14:57',"%Y-%m-%d %H:%M:%S")
    channel_name = "#grbalerttest"
    channel_name_private = "#grbalerttest"

elif mode == 'observation':
    currenttime_time  = time.gmtime()
    channel_name = "#grbalerts"  
    channel_name_private = "#grbalerttest" 

if location == 'beijing':
    configuration_file = homedir+'/configuration_bj.dat'
elif location == 'xinglong':
    configuration_file = homedir+'/configuration_xl.dat'    


mode_para = [mode,configuration_file,channel_name,currenttime_time,channel_name_private]
# print mode_para

""" main method """
usage = "Example: python ToO_GW_O3_followup.py" 

# configuration_file_dev = open(configuration_file,'rU')

# lines1=configuration_file_dev.read().splitlines()
# configuration_file_dev.close()

# for line1 in lines1:
#     word=line1.split()
#     if word[0] == 'followup_gwac_ip':
#         followup_tele60_ip = word[2]
#     elif word[0] == 'followup_telesocket_port':
#         followup_telesocket_port = int(word[2])
#     elif word[0] == 'gcnfollowupuser':
#         gcnfollowupuser = word[2]
#     elif word[0] == 'gcnfollowupip':
#         gcnfollowupip = word[2]
#     elif word[0] == 'gcnfollowupmypassword':
#         gcnfollowupmypassword = word[2]
#     elif word[0] == 'gcnfollowupmydb':
#         gcnfollowupmydb = word[2]
#     elif word[0] == 'griduser':
#         griduser = word[2]
#     elif word[0] == 'gridip':
#         gridip = word[2]
#     elif word[0] == 'gridmypassword':
#         gridmypassword = word[2]
#     elif word[0] == 'gridmydb':
#         gridmydb = word[2]
#     elif word[0] == 'group_grid':
#         group_grid = word[2]
#     elif word[0] == 'unit_id':
#         unit_id = word[2]

# group_id = group_grid.split("|")[0]
# grid_id = group_grid.split("|")[1]
# if group_id == "GWAC_XL1":
#     group_id = '001'

# conn_gwacoc_too = MySQLdb.connect(host = gcnfollowupip,
#                     user = gcnfollowupuser,
#                     passwd = gcnfollowupmypassword,
#                     db =  gcnfollowupmydb)  

# Too_trigger_table = 'gwac_trigger'

# cursor_gwacoc_too = conn_gwacoc_too.cursor ()
# cursor_gwacoc_too.execute("select MAX(id) from " + Too_trigger_table )
# extract_result = cursor_gwacoc_too.fetchall()
# cursor_gwacoc_too.close ()
# conn_gwacoc_too.close()
# if len(extract_result) > 0:
#     id_old = extract_result[0][0]
#     if id_old == None: id_old = 0
# i = id_old
# id_old = 0   
# # print i     


# ask the DB to find the last alert
old_trigger_params = json.loads(Retrieve_last_trigger_params())
old_trigger_ID = old_trigger_params['ID_external_trigger']   
old_trigger_ID = 525
while True: 
    if mode == 'observation':
        currenttime_time  = time.gmtime()
        # currenttime_time = time.strptime('2018-06-01 19:14:57',"%Y-%m-%d %H:%M:%S")
        mode_para = [mode,configuration_file,channel_name,currenttime_time,channel_name_private]

    currenttime_time  = time.gmtime()
    mode_para = [mode,configuration_file,channel_name,currenttime_time,channel_name_private]

    time_mark = time.strftime("%Y-%m-%d %H:%M:%S", currenttime_time)

    new_trigger_params = json.loads(Retrieve_newer_trigger_params(old_trigger_ID))


    trigger_ID = new_trigger_params['ID_external_trigger']
    trigger_name = new_trigger_params['external_trigger_name']
    trigger_time = new_trigger_params['external_trigger_time']  
    trigger_type = new_trigger_params['ID_external_trigger_type'] 
    trigger_type_name = new_trigger_params['ID_external_trigger_type']
    alert_message_revision = new_trigger_params['alert_message_revision']        
    alert_message_type = new_trigger_params['alert_message_type']

    print(time_mark,old_trigger_ID)
    if len(trigger_ID) > 0:
        print('new alert: ')
        for n in range(len(trigger_ID)):
            pointing_list_params = json.loads(Retrieve_pointing_list(35))
            name_telescope = pointing_list_params['name_telescope'][-1]

            if trigger_type[n] == 4:
                print('GW trigger (test):',trigger_name[n],trigger_ID[n],alert_message_type[n])
                mark1 = 10000
                mark5 = 1
            elif trigger_type[n] == 5:
                print('GW trigger (real):',trigger_name[n],trigger_ID[n],alert_message_type[n])
                mark1 = 10000
                mark5 = 2

            if alert_message_type[n] == 'Initial':
                print('Initial')
                mark2 = 1000
            if alert_message_type[n] == 'Update':
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
            datet_datetime = datetime.datetime.strptime(trigger_time[n], '%Y-%m-%d %H:%M:%S')
            timedelta = utc_datetime - datet_datetime
            timedelta_seconds = timedelta.total_seconds()
            timedelay_check = timedelta_seconds / 60.0

            print('time delay',timedelay_check)
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

            #----- Retrieve the pointing list sent from LAL
            ID_grid = []
            ID_field = []
            RA_pointing = []
            dec_pointing = []
            grade_pointing = []
            # pointing_list_params = json.loads(Retrieve_pointing_list(db,trigger_ID[n]))
            pointing_list_params = json.loads(Retrieve_pointing_list(35))
            ID_grid = pointing_list_params['ID_grid']
            ID_field = pointing_list_params['ID_field']
            RA_pointing = pointing_list_params['RA_pointing']
            dec_pointing = pointing_list_params['dec_pointing']
            grade_pointing = pointing_list_params['grade_pointing']

            obs_type_list = json.loads(Telescope_Obs_strategy_match(mark,location))
            # print(obs_type_list)
            task_for = 0
            if 12 in obs_type_list['obs_type_id']:
                task_for = task_for + 1
            if 13 in obs_type_list['obs_type_id']:
                task_for = task_for + 2
            if 14 in obs_type_list['obs_type_id']:   
                task_for = task_for + 5

            if task_for == 1:
                print('it is a gwac task')  
                print(obs_type_list['obs_type_id'],obs_type_list['priority'],obs_type_list['group_ids'],obs_type_list['unit_ids'])
                ToP_GW_followup_GWAC_tile_assign(utc_datetime,obs_type_list['group_ids'],obs_type_list['unit_ids'],\
                    pointing_list_params['ID_grid'],pointing_list_params['ID_field'],pointing_list_params['RA_pointing'],pointing_list_params['dec_pointing'],pointing_list_params['grade_pointing'])             
            elif task_for == 2:
                print('it is a F60 task')
                print(obs_type_list['obs_type_id'],obs_type_list['priority'],obs_type_list['group_ids'],obs_type_list['unit_ids'])
                ToP_GW_followup_F60_F30_galaxy_assign(utc_datetime,obs_type_list['group_ids'],obs_type_list['unit_ids'],\
                    pointing_list_params['ID_grid'],pointing_list_params['ID_field'],pointing_list_params['RA_pointing'],pointing_list_params['dec_pointing'],pointing_list_params['grade_pointing'])             
            elif task_for == 5:
                print('it is a F30 task')
                print(obs_type_list['obs_type_id'],obs_type_list['priority'],obs_type_list['group_ids'],obs_type_list['unit_ids'])
                ToP_GW_followup_F60_F30_galaxy_assign(utc_datetime,obs_type_list['group_ids'],obs_type_list['unit_ids'],\
                    pointing_list_params['ID_grid'],pointing_list_params['ID_field'],pointing_list_params['RA_pointing'],pointing_list_params['dec_pointing'],pointing_list_params['grade_pointing'])             
            elif task_for == 7:
                print('it is a F60 and F30 task') 
                print(obs_type_list['obs_type_id'],obs_type_list['priority'],obs_type_list['group_ids'],obs_type_list['unit_ids'])
                ToP_GW_followup_F60_F30_galaxy_assign(utc_datetime,obs_type_list['group_ids'],obs_type_list['unit_ids'],\
                    pointing_list_params['ID_grid'],pointing_list_params['ID_field'],pointing_list_params['RA_pointing'],pointing_list_params['dec_pointing'],pointing_list_params['grade_pointing'])             


            # print('pointing list:')
            # if len(ID_grid) > 0:
            #     for m in range(len(ID_grid)):            
            #         print('   ',ID_grid[m],ID_field[m],RA_pointing[m],dec_pointing[m],grade_pointing[m]) 




            old_trigger_ID = trigger_ID[n]

    time.sleep(3)


    # # date_mark = time.strftime("%Y-%m-%d", currenttime_time)

    # # conn_gwacoc_too = MySQLdb.connect(host = gcnfollowupip,
    # #                 user = gcnfollowupuser,
    # #                 passwd = gcnfollowupmypassword,
    # #                 db =  gcnfollowupmydb)  
    # # cursor_gwacoc_too = conn_gwacoc_too.cursor ()
    # # cursor_gwacoc_too.execute("select * from " + Too_trigger_table + " ORDER BY id DESC LIMIT 1")
    # # extract_result = cursor_gwacoc_too.fetchall()
    # # cursor_gwacoc_too.close ()
    # # conn_gwacoc_too.close()
    # # # print extract_result
    # # if len(extract_result) > 0:
    # #     id_lastest = extract_result[0][0]
    # #     rolet = extract_result[0][3]
    # #     if id_lastest == None: id_lastest = 0
    # # if(i == id_lastest):
    # #     print time_mark + ' no new trigger'
    # #     i = id_lastest
    # # elif(i <= id_lastest and rolet == 'observation'):
    # #     print time_mark + ' new trigger coming'

    #     packet_type = extract_result[0][1]
    #     pkt_ser_num = extract_result[0][2]
    #     rolet = extract_result[0][3]
    #     TrigID = extract_result[0][6]
    #     obs_id = packet_type + "_" + pkt_ser_num + "_" + TrigID + "_" + rolet
    #     print obs_id

    #     if packet_type in ['1151','1152']:
    #         GWAC_GW_followup(mode_para,packet_type,pkt_ser_num,TrigID,rolet,group_id,unit_id,obs_id)
    #     elif packet_type in ['Antares']:
    #         longitude = extract_result[0][10]
    #         latitude = extract_result[0][11]
    #         GWAC_neutrino_followup(mode_para,obs_id,longitude,latitude)
    #     elif packet_type in ['111']:
    #         longitude = extract_result[0][10]
    #         latitude = extract_result[0][11]
    #         err_size = extract_result[0][12] 
    #         probability = extract_result[0][17]
    #         obs_threshold = 95
    #         if (float(probability) >= obs_threshold) :
    #             print probability
    #             obs_mark = GWAC_GBM_gwacfollowup(mode_para,obs_id,group_id,unit_id,TrigID,grid_id,longitude,latitude,err_size)                
    #             release_range = 'public'
    #             func_GRBalerts_slack(mode_para,id_lastest,TrigID,release_range)
    #             # threading.Thread(target= func_GRBalerts_slack(mode_para,id_lastest,TrigID)).start()
    #             # threading.Thread(target=GWAC_GBM_gwacfollowup(mode_para,obs_id,group_id,unit_id,TrigID,grid_id,longitude,latitude,err_size)).start()
    #         else:
    #             STATUS = 'A new alert is coming. \n This alert is currently unobservable because of low probability. \n The details of the alert: \n'
    #             release_range = 'private'
    #             func_slack_message(mode_para,STATUS,release_range)
    #             time.sleep(5)
    #             func_GRBalerts_slack(mode_para,id_lastest,TrigID,release_range)
    #     elif packet_type in ['112']: 
    #         sig2no = extract_result[0][16]
    #         obs_threshold = 95
    #         check_keyword = 'probability'
    #         obs_status = func_GBM_111_observable_check(mode_para,homedir,check_keyword,obs_threshold,TrigID)
    #         if obs_status == 'observable':
    #             STATUS = 'TrigID: '+ TrigID + '. Sig2no: ' + sig2no + '.\n'
    #             release_range = 'public'
    #             func_slack_message(mode_para,STATUS,release_range)
    #     elif packet_type in ['115']:
    #         longitude = extract_result[0][10]
    #         latitude = extract_result[0][11]
    #         err_size = extract_result[0][12] 
    #         probability = extract_result[0][17]
    #         obs_threshold = 95
    #         check_keyword = 'probability'
    #         obs_status = func_GBM_111_observable_check(mode_para,homedir,check_keyword,obs_threshold,TrigID)
    #         if obs_status == 'observable':
    #             obs_mark = GWAC_GBM_gwacfollowup(mode_para,obs_id,group_id,unit_id,TrigID,grid_id,longitude,latitude,err_size)
    #             release_range = 'public'  
    #             func_GRBalerts_slack(mode_para,id_lastest,TrigID,release_range) 
    #         elif obs_status == 'unobservable':   
    #             STATUS = 'A new alert is coming. \n This alert is currently unobservable because of low probability. \n The details of the alert: \n'
    #             release_range = 'private'
    #             func_slack_message(mode_para,STATUS,release_range)
    #             time.sleep(5)
    #             func_GRBalerts_slack(mode_para,id_lastest,TrigID,release_range)
    #     elif packet_type in ['61','67']:      
    #         longitude = extract_result[0][10]
    #         latitude = extract_result[0][11]
    #         err_size = extract_result[0][12] 
    #         sig2no = extract_result[0][16]               
    #         mark_obs = tele60_GRB_followup(mode_para,tele60_followup_codepath,id_lastest,TrigID,packet_type,pkt_ser_num,longitude,latitude,err_size,sig2no)
    #         time.sleep(5)
    #         if mark_obs == 'obs':
    #             release_range = 'public'
    #             func_GRBalerts_slack(mode_para,id_lastest,TrigID,release_range)
    #         else:
    #             release_range = 'private'
    #             func_GRBalerts_slack(mode_para,id_lastest,TrigID,release_range)
    #     i = id_lastest
    # else:
    #     i = id_lastest
    # # print i

    
    # time.sleep(3)
