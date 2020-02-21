import sys, stat
from os.path import exists, join
import os.path
from os import pathsep
import time
import datetime
import numpy as np
# from astro_parameter_calculate import astro_parameter_calculate
import ephem

try:
  sys.path.append("./jdcal/")
  from jdcal import gcal2jd
  from jdcal import jd2gcal
except:
  print("please install jdcal code ")
  sys.exit()
from astropy.table import Table,Column
import pandas as pd

def Map(Func, List):
    if sys.version_info.major == 2:
        new = map(Func, List)
    else:
        new = []
        for i in map(Func, List):
            new.append(i)
    return new

def func_object_sort(rows_scheduled, current_utc_datetime):
    delta_datetime = []
    for m in rows_scheduled['insert_time']:
        insert_time_datetime = datetime.datetime.strptime(m, "%Y/%m/%dT%H:%M:%S")
        delta_datetime.append((current_utc_datetime - insert_time_datetime).total_seconds())
    # print(delta_datetime)

    delta_datetime = Column(data=delta_datetime,name=('delta_datetime'),dtype=('f'))
    if len(rows_scheduled['obj_id']) > 0:
        rows_scheduled.add_column(delta_datetime)
        data_new = rows_scheduled

        # data=Table(new_data,names=('obj_id', 'radeg', 'decdeg','priority','mjd_begin','mjd_end','objrank','mode','insert_time','delta_datetime'),dtype=('f', 'f', 'f','f','f','f','f','a','a','f'))
        order_index = np.lexsort((data_new['objra'],data_new['priority']))
        order_index_reverse = order_index[::-1]
        # data_order = data[order_index_reverse]
        data_order = data_new[order_index_reverse]

        gwac_index = np.where((data_order['priority'] >= 0) & (data_order['priority'] <= 19))[0]
        if len(gwac_index) > 0:
            data_gwac = data_order[gwac_index]
        oba_obm_index = np.where((data_order['priority'] >= 20) & (data_order['priority'] <= 29))[0]
        if len(oba_obm_index) > 0:
            data_oba_obm = data_order[oba_obm_index]
        rot_low_index = np.where((data_order['priority'] >= 30) & (data_order['priority'] <= 39))[0]
        if len(rot_low_index) > 0:
            data_rot_low = data_order[rot_low_index]
        goa_gom_index = np.where((data_order['priority'] >= 40) & (data_order['priority'] <= 59))[0]
        if len(goa_gom_index) > 0:
            data_goa_gom = data_order[goa_gom_index]
        toa_tom_index = np.where((data_order['priority'] >= 60) & (data_order['priority'] <= 79))[0]
        if len(toa_tom_index) > 0:
            data_toa_tom = data_order[toa_tom_index]
        rot_high_index = np.where((data_order['priority'] >= 80) & (data_order['priority'] <= 89))[0]
        if len(rot_high_index) > 0:
            data_rot_high = data_order[rot_high_index]
        cal_index = np.where((data_order['priority'] >= 90) & (data_order['priority'] <= 99))[0]
        if len(cal_index) > 0:
            data_cal = data_order[cal_index]
        man_index = np.where(data_order['priority'] >= 100)[0]
        if len(man_index) > 0:
            data_man = data_order[man_index]

        # data_order_new = []
        data_order_new = Table(names=('obj_id', 'tw_begin', 'tw_end','mode'),dtype=('a19','a19','a19','a19'))
        if len(man_index) > 0:
            # print('data_man',data_man)
            for m in range(len(data_man)):
                data_order_new.add_row([data_man[m]['obj_id'],data_man[m]['tw_begin'],data_man[m]['tw_end'],data_man[m]['mode']])
        if len(cal_index) > 0:
            # print('data_cal',data_cal)
            for m in range(len(data_cal)):
                data_order_new.add_row([data_cal[m]['obj_id'],data_cal[m]['tw_begin'],data_cal[m]['tw_end'],data_cal[m]['mode']])
        if len(rot_high_index) > 0:
            # print('data_rot',data_rot)
            for m in range(len(data_rot_high)):
                data_order_new.add_row([data_rot_high[m]['obj_id'],data_rot_high[m]['tw_begin'],data_rot_high[m]['tw_end'],data_rot_high[m]['mode']])
        if len(toa_tom_index) > 0:
            toa_tom_order_index = np.lexsort((data_toa_tom['objrank'],data_toa_tom['priority'])) # ordering by ranking and priority
            toa_tom_order_index_reverse = toa_tom_order_index[::-1]
            data_toa_tom_order = data_toa_tom[toa_tom_order_index_reverse]
            for m in range(len(data_toa_tom_order)):
                data_order_new.add_row([data_toa_tom_order[m]['obj_id'],data_toa_tom_order[m]['tw_begin'],data_toa_tom_order[m]['tw_end'],data_toa_tom_order[m]['mode']])
        if len(goa_gom_index) > 0:
            goa_gom_order_index = np.lexsort((data_goa_gom['delta_datetime'],data_goa_gom['priority'])) # ordering by insert_time and priority
            goa_gom_order_index_reverse = goa_gom_order_index[::-1]
            data_goa_gom_order = data_goa_gom[goa_gom_order_index_reverse]
            # print('data_goa_gom',data_goa_gom_order)
            for m in range(len(data_goa_gom_order)):
                data_order_new.add_row([data_goa_gom_order[m]['obj_id'],data_goa_gom_order[m]['tw_begin'],data_goa_gom_order[m]['tw_end'],data_goa_gom_order[m]['mode']])
        if len(rot_low_index) > 0:
            # print('data_rot',data_rot)
            for m in range(len(data_rot_low)):
                data_order_new.add_row([data_rot_low[m]['obj_id'],data_rot_low[m]['tw_begin'],data_rot_low[m]['tw_end'],data_rot_low[m]['mode']])
        if len(oba_obm_index) > 0: # ording by ra
            oba_obm_order_index = np.lexsort((data_oba_obm['objra'],data_oba_obm['objrank'],data_oba_obm['priority']))
            oba_obm_order_index_reverse = oba_obm_order_index[::-1]
            data_oba_obm_order = data_oba_obm[oba_obm_order_index_reverse]
            for m in range(len(data_oba_obm_order)):
                data_order_new.add_row([data_oba_obm_order[m]['obj_id'],data_oba_obm_order[m]['tw_begin'],data_oba_obm_order[m]['tw_end'],data_oba_obm_order[m]['mode']])
        if len(gwac_index) > 0: # ording by ra
            gwac_order_index = np.lexsort((data_gwac['objra'],data_gwac['objrank']))
            gwac_order_index_reverse = gwac_order_index[::-1]
            data_gwac_order = data_gwac[gwac_order_index_reverse]
            # print(type(data_gwac_order),gwac_order_index)
            for m in range(len(data_gwac_order)):
                data_order_new.add_row([data_gwac_order[m]['obj_id'],data_gwac_order[m]['tw_begin'],data_gwac_order[m]['tw_end'],data_gwac_order[m]['mode']])
    else:
        data_order_new = Table(names=('obj_id', 'tw_begin', 'tw_end','mode'),dtype=('a19','a19','a19','a19'))
    return data_order_new #``


# conf_obs_parameters_sys = './conf_obs_parameters_sys.dat'
# conf_obs_parameters_sys_dev = open(conf_obs_parameters_sys,'rU')

# lines2=conf_obs_parameters_sys_dev.read().splitlines()
# conf_obs_parameters_sys_dev.close()

# for line2 in lines2:
#     word=line2.split()
#     if word[0] == 'observatory_lat':
#         observatory_lat = word[2]
#     elif word[0] == 'observatory_lon':
#         observatory_lon = word[2]
#     elif word[0] == 'observatory_elevation':
#         observatory_elevation = float(word[2])

# # read telescope pointing constrain data -----------------------------------------
# tele_point_table = "tele_pointing_constrain.csv"
# tele_pointing_constrain_dframe_all = pd.read_csv(tele_point_table)

# homedir = os.getcwd()
# conf_obs_parameters_sys = './conf_obs_parameters_sys.dat'
# conf_obs_parameters_sys_dev = open(conf_obs_parameters_sys,'rU')

# conf_obs_parameters=conf_obs_parameters_sys_dev.read().splitlines()
# conf_obs_parameters_sys_dev.close()

# # set observatory parameters ----------------------------------------
# observatory = ephem.Observer()
# observatory.lat = observatory_lat
# observatory.lon = observatory_lon
# observatory.elevation = observatory_elevation
# lat_dd = float(str(observatory.lat).split(":")[0])+\
# float(str(observatory.lat).split(":")[1])/60.0+\
# float(str(observatory.lat).split(":")[2])/3600.0

# obj_id = [6977, 6981, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652, 653, 654, 677, 678, 679, 680]
# racen = [328.93, 24.03, 18.0238, 18.0238, 18.0238, 64.2186, 64.2186, 64.2186, 64.2186, 119.278, 119.278, 119.278, 119.278, 123.321, 123.321, 123.321, 123.321, 157.827, 157.827, 157.827, 157.827, 165.602, 165.602, 165.602, 165.602, 171.358, 171.358, 171.358, 171.358, 184.467, 184.467, 338.152, 338.152, 338.152, 338.152]
# deccen =  [59.5, 76.5, 22.7442, 22.7442, 22.7442, 1.08986, 1.08986, 1.08986, 1.08986, 9.94301, 9.94301, 9.94301, 9.94301, 22.6483, 22.6483, 22.6483, 22.6483, 50.8933, 50.8933, 50.8933, 50.8933, -6.70182, -6.70182, -6.70182, -6.70182, -8.46636, -8.46636, -8.46636, -8.46636, 30.1168, 30.1168, 11.7309, 11.7309, 11.7309, 11.7309]
# priority = [10, 10, 10, 10, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20]
# obj_id = [6977,6981,2,3,4,5,6,7,8,9,10,11,12,13]
# group_id = ["XL001","XL001","XL001","XL001","XL002","XL002","XL002|XL003","XL002","XL002","XL003","XL002","XL003","XL003","XL003"]
# unit_id = [2,1,1,1,1,1,1,1,1,1,1,1,1,1]
# racen = [328.93, 24.03,250,160,212,252,295,212,323,241,270,318,289,338]
# deccen = [59.5, 76.5,60,10,50,80,20,30,70,70,40,20,60,11.7]
# objrank = [0.20,0.20,0.40,0.40,0.40,0.30,0.83,0.15,0.33,0.68,0.29,0.33,1.47,2.20]
# priority = [60,60,10,60,60,60,13,15,30,60,29,33,60,60]
# mode = [0,0,0,0,0,0,0,0,0,0,0,0,0,0]

# obj_id = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','6977','6981','2','3','4','5','6','7','8','9','10','11','12','13','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','6977','6981','2','3','4','5','6','7','8','9','10','11','12','13']
# group_id = ["XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL002","XL002","XL002|XL003","XL002","XL002","XL003","XL002","XL003","XL003","XL003","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL001","XL002","XL002","XL002|XL003","XL002","XL002","XL003","XL002","XL003","XL003","XL003"]
# unit_id = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1]
# racen = [10.64,8.81,6.26,4.55,9.69,7.98,214.52,243.12,216.46,189,197.64,178.81,20.26,184.55,19.69,227.98,214.52,3.12,216.46,189,291.75,310.57,314.82,261,75.0,165.0,328.93, 24.03,250,160,212,252,295,212,323,241,270,318,289,338,10.64,8.81,6.26,4.55,9.69,7.98,214.52,243.12,216.46,189,197.64,178.81,20.26,184.55,19.69,227.98,214.52,3.12,216.46,189,291.75,310.57,314.82,261,75.0,165.0,328.93, 24.03,250,160,212,252,295,212,323,241,270,318,289,338]
# deccen = [40.5,25.5,-42.5,-42.5,42.5,42.5,59.5,59.5,25.5,8.5,25.5,25.5,42.5,42.5,42.5,42.5,59.5,59.5,25.5,8.5,25.5,25.5,42.5,8.5,42.5,70.5,59.5, 76.5,60,10,50,80,20,30,70,70,40,20,60,11.7,40.5,25.5,-42.5,-42.5,42.5,42.5,59.5,59.5,25.5,8.5,25.5,25.5,42.5,42.5,42.5,42.5,59.5,59.5,25.5,8.5,25.5,25.5,42.5,8.5,42.5,70.5,59.5, 76.5,60,10,50,80,20,30,70,70,40,20,60,11.7]
# objrank = [0.2264,0.1831,0.1174,0.1047,0.0349,0.0277,0.0218,0.0206,0.019,0.0113,0.2264,0.1831,0.1174,0.1047,0.0349,0.0277,0.0218,0.0206,0.019,0.0113,0.3009,0.2837,0.2415,0.232,0.51,0.24,0.20,0.20,0.40,0.40,0.40,0.30,0.83,0.15,0.33,0.68,0.29,0.33,1.47,2.20,0.2264,0.1831,0.1174,0.1047,0.0349,0.0277,0.0218,0.0206,0.019,0.0113,0.2264,0.1831,0.1174,0.1047,0.0349,0.0277,0.0218,0.0206,0.019,0.0113,0.3009,0.2837,0.2415,0.232,0.51,0.24,0.20,0.20,0.40,0.40,0.40,0.30,0.83,0.15,0.33,0.68,0.29,0.33,1.47,2.20]
# priority = [42,41,42,42,42,42,42,42,42,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,42,42,60,60,10,60,60,60,13,15,30,60,29,33,60,60,42,41,42,42,42,42,42,42,42,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,42,42,60,60,10,60,60,60,13,15,30,60,29,33,60,60]
# mode = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
# insert_time = ['2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T18:25:00','2019/09/28T21:26:00','2019/09/28T20:47:00','2019/09/28T20:28:00','2019/09/28T20:59:00','2019/09/28T17:30:00','2019/09/28T20:31:00','2019/09/28T22:32:00','2019/09/28T20:33:00','2019/09/28T20:34:00','2019/09/28T20:35:00','2019/09/28T20:36:00','2019/09/28T20:37:00','2019/09/28T20:38:00','2019/09/28T20:39:00','2019/09/28T20:40:00','2019/09/28T20:41:00','2019/09/28T20:42:00','2019/09/28T20:43:00','2019/09/28T20:44:00','2019/09/28T18:45:00','2019/09/28T20:46:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T18:25:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T18:25:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T18:25:00','2019/09/28T21:26:00','2019/09/28T20:47:00','2019/09/28T20:28:00','2019/09/28T20:59:00','2019/09/28T17:30:00','2019/09/28T20:31:00','2019/09/28T22:32:00','2019/09/28T20:33:00','2019/09/28T20:34:00','2019/09/28T20:35:00','2019/09/28T20:36:00','2019/09/28T20:37:00','2019/09/28T20:38:00','2019/09/28T20:39:00','2019/09/28T20:40:00','2019/09/28T20:41:00','2019/09/28T20:42:00','2019/09/28T20:43:00','2019/09/28T20:44:00','2019/09/28T18:45:00','2019/09/28T20:46:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T18:25:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00','2019/09/28T18:25:00','2019/09/28T20:01:00','2019/09/28T20:12:00','2019/09/28T20:23:00','2019/09/28T20:54:00']

# # current_utc_datetime = datetime.datetime.utcnow()
# currenttime_utc_time = time.strptime('2019-09-28 20:20:00',"%Y-%m-%d %H:%M:%S")
# current_utc_str = time.strftime("%Y-%m-%d %H:%M:%S", currenttime_utc_time)
# current_utc_datetime = datetime.datetime.strptime(current_utc_str, "%Y-%m-%d %H:%M:%S")

# observatory.date = current_utc_str

# # calculate local sidereal time  ----------------------------------------
# current_lst_dd = float(str(observatory.sidereal_time()).split(":")[0])* 15.0+\
# float(str(observatory.sidereal_time()).split(":")[1])/60.0* 15.0+\
# float(str(observatory.sidereal_time()).split(":")[2])/3600.0* 15.0

# data = func_object_sort(obj_id,group_id,unit_id,racen,deccen,objrank,priority,mode,insert_time,current_utc_datetime,tele_pointing_constrain_dframe_all,conf_obs_parameters,observatory)
# # print(data)
# # print(observatory.date,'current lst',current_lst_dd,observatory.sidereal_time())
# for n in range(len(data)): #`` data[0]
#     if float(data[n]['mjd_begin']) > 0.0:
#         date_begin = jd2gcal(2400000.5, float(data[n]['mjd_begin'])) #`` float
#         date_end = jd2gcal(2400000.5, float(data[n]['mjd_end'])) #`` flaot
#         # print((data[n][4]),float(data[n][5]))
#         begin_hh = int(date_begin[3] * 24.0 ) #``
#         begin_mm = int((date_begin[3] * 24.0 - begin_hh)*60.0) #``                          The last element of the tuple is the same as
#         begin_ss = (((date_begin[3] * 24.0 - begin_hh)*60.0) - begin_mm)*60.0 #``              (hh + mm / 60.0 + ss / 3600.0) / 24.0  
#         end_hh = int(date_end[3] * 24.0 ) #``                                               where hh, mm, and ss are the hour, minute and second of the day.
#         end_mm = int((date_end[3] * 24.0 - end_hh)*60.0) #``
#         end_ss = (((date_end[3] * 24.0 - end_hh)*60.0) - end_mm)*60.0 #``
#         calendar_date_begin = "%d/%02d/%02d %02d:%02d:%02d" % (date_begin[0],date_begin[1],date_begin[2],begin_hh,begin_mm,begin_ss) #``
#         calendar_date_end = "%d/%02d/%02d %02d:%02d:%02d" % (date_end[0],date_end[1],date_end[2],end_hh,end_mm,end_ss) #``
#     else:
#         calendar_date_begin = "0000/00/00 00:00:00"
#         calendar_date_end = "0000/00/00 00:00:00"

#     hour_angle_current = (current_lst_dd - data[n]['radeg'])/15.0
#     if hour_angle_current >= 12.0 and hour_angle_current <= 24.0:
#     	hour_angle_current = 1 * (hour_angle_current - 24.0 )
#     if hour_angle_current <= -12.0 and hour_angle_current >= -24.0:
#     	hour_angle_current = 1 * (hour_angle_current + 24.0)
#     print(n,str(int(data[n]['obj_id'])),("ra %4.1f" % data[n]['radeg']),("dec %4.1f" % data[n]['decdeg']),data[n]['objrank'],data[n]['priority'],current_utc_datetime,calendar_date_begin,calendar_date_end,("%4.1f" % hour_angle_current),data[n]['insert_time'])





