# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""
__author__='Xuhui Han'
__version__ ='$Revision: 1.1 $'
import sys, stat
import math
from os.path import exists, join
import os.path
from os import pathsep
import string
import os
import datetime
import numpy as np
import time
import re
import numpy as np
from coords import gal2eq
from coords import eq2gal
from angular_distance import angular_distance
import ephem
import pandas as pd
# try:
#   sys.path.append("./astronomical_cal_code/")
#   import dt2jd
#   import jd2dt
# except:
#   print("please install sidereal code ")
#   sys.exit()
try:
  sys.path.append("./jdcal/")
  from jdcal import gcal2jd
  from jdcal import jd2gcal
except:
  print("please install jdcal code ")
  sys.exit()

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def func_coor_obs_timewindow_calculate(Obj_ID,racen, deccen,MJD_time_current):

	MJD_current = float(int(MJD_time_current))
	# print(MJD_current,MJD_time_current)

	homedir = os.getcwd()
	conf_obs_parameters_sys = './conf_obs_parameters_sys.dat'
	conf_obs_parameters_sys_dev = open(conf_obs_parameters_sys,'rU')

	lines2=conf_obs_parameters_sys_dev.read().splitlines()
	conf_obs_parameters_sys_dev.close()

	for line2 in lines2:
		word=line2.split()
		if word[0] == 'observatory_lat':
		    observatory_lat = word[2]
		elif word[0] == 'observatory_lon':
		    observatory_lon = word[2]
		elif word[0] == 'observatory_elevation':
		    observatory_elevation = float(word[2])
		elif word[0] == 'zenith_sun_min':
		    zenith_sun_min = float(word[2])	        
		elif word[0] == 'zenith_min':
		    zenith_min = float(word[2])
		elif word[0] == 'gal_min':
		    gal_min = float(word[2])
		elif word[0] == 'moon_dis_min_para':
			moon_dis_min_str = word[2]
			moon_dis_para_str = moon_dis_min_str.split('|')
			moon_dis_phase_data = []
			for moon_dis_para in moon_dis_para_str:
				moon_dis_para_phase_min = float(moon_dis_para.split(':')[0].split('-')[0])
				moon_dis_para_phase_max = float(moon_dis_para.split(':')[0].split('-')[1])
				moon_dis_para_dis = float(moon_dis_para.split(':')[1])
				moon_dis_phase_data.append([moon_dis_para_phase_min,moon_dis_para_phase_max,moon_dis_para_dis])
			# moon_dis_phase_data = filter(None,moon_dis_phase_data)

	path = './'
	# # set observation day ------------------------------------------------
	# # current_utc_datetime = datetime.datetime.utcnow()
	# Op_time = current_utc_datetime.strftime( '%Y-%m-%d' )  
	# Op_time = time.strptime( Op_time, "%Y-%m-%d") 
	# gcal_y = Op_time.tm_year
	# gcal_m = Op_time.tm_mon
	# gcal_d = Op_time.tm_mday 
	# MJD_current = gcal2jd(gcal_y,gcal_m,gcal_d)[1] 
	# MJD_current = MJD_newyear
	# date_current = jd2gcal(2400000.5, MJD_current)
	# calendar_d_lable = "%d_%d_%d" % (date_current[0],date_current[1],date_current[2])
	# calendar_d = "%d-%d-%d" % (date_current[0],date_current[1],date_current[2])

	# print calendar_d

	# start calculate observation sequence
	time_interval = 20.0 # 20 munitues
	night_number = 72 # every 20 munitues, 72 in total.

	# set observatory parameters ----------------------------------------
	observatory = ephem.Observer()
	observatory.lat = observatory_lat
	observatory.lon = observatory_lon
	observatory.elevation = observatory_elevation
	lat_dd = float(str(observatory.lat).split(":")[0])+\
	float(str(observatory.lat).split(":")[1])/60.0+\
	float(str(observatory.lat).split(":")[2])/3600.0

	# set mjd time ----------------------------------------
	nighttime_current = jd2gcal(2400000.5, MJD_time_current)
	hh = int(nighttime_current[3] * 24.0 )
	mm = int((nighttime_current[3] * 24.0 - hh)*60.0)
	ss = (((nighttime_current[3] * 24.0 - hh)*60.0) - mm)*60.0
	hms = "%02d:%02d:%0.1f" % (hh,mm,ss)
	nighttime_current_cal = ('%s/%s/%s %s' % (nighttime_current[0],nighttime_current[1],nighttime_current[2],hms))
	nighttime_current_str = ('%s/%s/%sT%s' % (nighttime_current[0],nighttime_current[1],nighttime_current[2],hms))
	observatory.date = nighttime_current_cal
	

	# calculate local sidereal time  ----------------------------------------
	current_lst_dd = float(str(observatory.sidereal_time()).split(":")[0])* 15.0+\
	float(str(observatory.sidereal_time()).split(":")[1])/60.0* 15.0+\
	float(str(observatory.sidereal_time()).split(":")[2])/3600.0* 15.0
	# print(observatory.date,current_lst_dd)

	# calculate galactic coordinate of field center and all vertexes
	g_cen_lon_dd,g_cen_lat_dd = eq2gal(racen,deccen)

	galactic_lat_min = abs(g_cen_lat_dd)

	# obs_id_1 = []
	# racen_1 = []	
	# deccen_1 = []
	# priority_1 = []
	# ut_time_begin = []
	# ut_time_end = []
	# mjd_begin = []
	# mjd_end = []

	mjd = []
	ut_time = []
	lst = []
	zenith = []
	for n in range(night_number):	
		# set mjd time ----------------------------------------
		MJD_time = MJD_current + (n*time_interval/60.0/24.0)
		nighttime_current = jd2gcal(2400000.5, MJD_time)
		hh = int(nighttime_current[3] * 24.0 )
		mm = int((nighttime_current[3] * 24.0 - hh)*60.0)
		ss = (((nighttime_current[3] * 24.0 - hh)*60.0) - mm)*60.0
		hms = "%02d:%02d:%0.1f" % (hh,mm,ss)
		nighttime_current_cal = ('%s/%s/%s %s' % (nighttime_current[0],nighttime_current[1],nighttime_current[2],hms))
		nighttime_current_str = ('%s/%s/%sT%s' % (nighttime_current[0],nighttime_current[1],nighttime_current[2],hms))
		observatory.date = nighttime_current_cal
		# print(observatory.date)

		# # set local time ----------------------------------------
		local_nighttime_current = ephem.localtime(observatory.date)
		str(local_nighttime_current).replace(' ','T')

		# set UT time ----------------------------------------
		UT_nighttime_current = ephem.Date(observatory.date)
		UT_nighttime_current_str_T = UT_nighttime_current.datetime().strftime("%Y/%m/%d %H:%M:%S")

		# calculate local sidereal time  ----------------------------------------
		lst_dd = float(str(observatory.sidereal_time()).split(":")[0])* 15.0+\
		float(str(observatory.sidereal_time()).split(":")[1])/60.0* 15.0+\
		float(str(observatory.sidereal_time()).split(":")[2])/3600.0* 15.0
		# print(observatory.date,lst_dd)


		# calculate altitude angle or zenith angular distance of the sun   ---------------------------------
		solar = ephem.Sun(observatory)
		solar_alt_dd = 90 - float(str(solar.alt).split(":")[0])+float(str(solar.alt).split(":")[1])/60.0+float(str(solar.alt).split(":")[2])/3600.0
		#print('solar  %s' % (solar_alt_dd))
		

		lunar = ephem.Moon(observatory)
		lunar_ra_dd = float(str(lunar.ra).split(":")[0])* 15.0+float(str(lunar.ra).split(":")[1])/60.0* 15.0+float(str(lunar.ra).split(":")[2])/3600.0* 15.0
		lunar_dec_dd = float(str(lunar.dec).split(":")[0])+float(str(lunar.dec).split(":")[1])/60.0+float(str(lunar.dec).split(":")[2])/3600.0
		#print('lunar %s %s %s' % (lunar_ra_dd, lunar_dec_dd, lunar.moon_phase))

		# calculate zenith angular distance of field center and all vertexes
		zenith_ang_dis_cen_dd = (angular_distance(racen, deccen,lst_dd,lat_dd))

		# calculate angular distance between field center and all vertexes and moon
		moon_ang_dis_min = (angular_distance(racen, deccen,lunar_ra_dd,lunar_dec_dd))

		# set mini distance from the moon
		for mm in range(len(moon_dis_phase_data)):
			if (lunar.moon_phase >= moon_dis_phase_data[mm][0] and lunar.moon_phase < moon_dis_phase_data[mm][1]):
				moon_dis_min = moon_dis_phase_data[mm][2]
				break
		# if ( solar_alt_dd > zenith_sun_min ) and ( zenith_ang_dis_cen_dd < zenith_min ):
		# 	print(local_nighttime_current,zenith_ang_dis_cen_dd,zenith_min,MJD_time)
		if ( solar_alt_dd > zenith_sun_min ) and ( zenith_ang_dis_cen_dd < zenith_min ) and (moon_ang_dis_min > moon_dis_min ): # and (galactic_lat_min[0] > gal_min) and (moon_ang_dis_min > moon_dis_min ):		
			mjd.append(MJD_time)
			ut_time.append(UT_nighttime_current_str_T)
			lst.append(lst_dd)
			zenith.append(zenith_ang_dis_cen_dd)

	obs_cons = 0
	if (len(mjd) > 0 ):
		obs_mjd_begin_index = 0
		obs_mjd_end_index = 0
		for mmm in range(len(mjd)-1):
			m_gap = mjd[mmm+1] - mjd[mmm] 
			m_int = (2.0/24.0)
			if m_gap > m_int:
				obs_mjd_begin_index = mjd.index(mjd[mmm+1])
		if obs_mjd_begin_index == 0:
			obs_mjd_begin_index = mjd.index(min(mjd))
			obs_mjd_end_index = mjd.index(max(mjd))
			mjd_begin = mjd[obs_mjd_begin_index]
			mjd_end = mjd[obs_mjd_end_index]
			lst_begin = lst[obs_mjd_begin_index]
			lst_end = lst[obs_mjd_end_index]
			zenith_begin = zenith[obs_mjd_begin_index]
			zenith_end = zenith[obs_mjd_end_index]
		obs_cons = 1
		print(Obj_ID,mjd_begin,mjd_end)

		# 	hour_ang_current = (current_lst_dd - racen) / 15.0
		# 	if hour_ang_current > 12:
		# 		hour_ang_current = hour_ang_current - 24
		# 	elif hour_ang_current < (-12):
		# 		hour_ang_current = 24 + hour_ang_current

		# 	hour_ang_west = ( current_lst_dd - lst_begin) / 15.0

		# 	if hour_ang_west > 12:
		# 		hour_ang_west = hour_ang_west - 24
		# 	elif hour_ang_west < (-12):
		# 		hour_ang_west = 24 + hour_ang_west

		# 	hour_ang_east = ( current_lst_dd - lst_end ) / 15.0
		# 	if hour_ang_east > 12:
		# 		hour_ang_east = hour_ang_east - 24
		# 	elif hour_ang_east < (-12):
		# 		hour_ang_east = 24 + hour_ang_east

		# 	hourangle_east = -5
		# 	hourangle_west = 5
		# 	if hour_ang_east < hourangle_east:
		# 		hour_ang_east_shift = hour_ang_east - hourangle_east	
		# 	else:
		# 		hour_ang_east_shift = 0
		# 	if hour_ang_west > hourangle_west:
		# 		hour_ang_west_shift = hour_ang_west - hourangle_west
		# 	else:
		# 		hour_ang_west_shift = 0

		# 	mjd_begin = mjd_begin + (hour_ang_east_shift / 24.0 / 15.0)
		# 	mjd_end = mjd_end + (hour_ang_west_shift / 24.0 / 15.0 )
		# 	# print(mjd_begin,mjd_end,MJD_time_current,hour_ang_east,hour_ang_west,hourangle_east,hourangle_west)
		# 	if MJD_time_current > mjd_begin and MJD_time_current < mjd_end:
		# 		obs_cons = 1
		# 		# mjd_begin = MJD_time_current + (hour_ang_east / 24.0 / 15.0)
		# 		# mjd_end = MJD_time_current + (hour_ang_west / 24.0 / 15.0 )
		# 	else:
		# 		obs_cons = 0
			
	if obs_cons == 0:
		mjd_begin = 0.0
		mjd_end = 0.0 

	# print(obs_cons,racen, deccen,mjd_begin,mjd_end)
	return obs_cons

# obs_id = 'sn2018hhn'
# racen = 343.133583
# deccen = 11.674083
# error_size = 0.1
# data = func_coor_obs_timewindow_calculate(obs_id,group_id,unit_it,racen,deccen,objrank,priority,MJD_current)
	
# # read telescope pointing constrain data -----------------------------------------
# tele_point_table = "tele_pointing_constrain.csv"
# tele_pointing_constrain_dframe_all = pd.read_csv(tele_point_table)
# group_id = "XL002"
# if group_id == "XL001":
# 	tele_pointing_constrain_dframe_1 =  tele_pointing_constrain_dframe_all[(tele_pointing_constrain_dframe_all["Group_ID"] == "XL001")].copy().reset_index(drop=True)
# 	tele_pointing_constrain_dframe =  tele_pointing_constrain_dframe_1[(tele_pointing_constrain_dframe_1["Unit_ID"] == 1)].copy().reset_index(drop=True)
# else:
# 	tele_pointing_constrain_dframe_1 =  tele_pointing_constrain_dframe_all[(tele_pointing_constrain_dframe_all["Group_ID"] == "XL002")].copy().reset_index(drop=True)
# 	tele_pointing_constrain_dframe =  tele_pointing_constrain_dframe_1[(tele_pointing_constrain_dframe_1["Unit_ID"] == 1)].copy().reset_index(drop=True)
# print(tele_pointing_constrain_dframe)


