# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""
__author__='Xuhui Han'
__version__ ='$Revision: 1.1 $'
import sys, stat
# import math
# from os.path import exists, join
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

# from astropy.coordinates import SkyCoord, EarthLocation
# from astropy import coordinates as coord
# from astropy.coordinates.tests.utils import randomly_sample_sphere
from astropy.time import Time
from astropy import units as u
import numpy as np
from astropy.table import Table,Column

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


def func_coor_obs_timewindow_calculate(obj_id,group_id,unit_id,racen,deccen,priority,MJD_time_current,tele_pointing_constrain_dframe_all,conf_obs_parameters,observatory):
	MJD_current = float(int(MJD_time_current))
	# print(MJD_current,MJD_time_current)

	for line2 in conf_obs_parameters:
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

	# start calculate observation sequence
	time_interval = 20.0 # 20 munitues
	night_number = 72 # every 20 munitues, 72 in total.

	# set observatory parameters ----------------------------------------
	# observatory = ephem.Observer()
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
	# print(observatory.date,'current lst 1',(current_lst_dd/15.0))

	# calculate galactic coordinate of field center and all vertexes
	g_cen_lon_dd,g_cen_lat_dd = eq2gal(racen,deccen)

	galactic_lat_min = abs(g_cen_lat_dd)

	data = Table(names=('obj_id', 'tw_begin', 'tw_end', 'obs_stag'),dtype=('a19','a19','a19','a19'))

	for i in range(len(racen)):

		# print(i,float(racen[i]),float(deccen[i]))

		if group_id[i] == "XL001":
			tele_pointing_constrain_dframe_1 =  tele_pointing_constrain_dframe_all[(tele_pointing_constrain_dframe_all["Group_ID"] == "XL001")].copy().reset_index(drop=True)
			tele_pointing_constrain_dframe =  tele_pointing_constrain_dframe_1[(tele_pointing_constrain_dframe_1["Unit_ID"] == 1)].copy().reset_index(drop=True)
			# print(tele_pointing_constrain_dframe)
		else:
			tele_pointing_constrain_dframe_1 =  tele_pointing_constrain_dframe_all[(tele_pointing_constrain_dframe_all["Group_ID"] == "XL002")].copy().reset_index(drop=True)
			tele_pointing_constrain_dframe =  tele_pointing_constrain_dframe_1[(tele_pointing_constrain_dframe_1["Unit_ID"] == 1)].copy().reset_index(drop=True)
		# print(group_id[i],'\n',tele_pointing_constrain_dframe)

		
		for m in range(len(tele_pointing_constrain_dframe)):
			if deccen[i] >= tele_pointing_constrain_dframe['dec_deg'][m] and deccen[i] < (tele_pointing_constrain_dframe['dec_deg'][m] + 10 ) :
				hourangle_east = (tele_pointing_constrain_dframe['hourangle_east'][m])
				hourangle_west = (tele_pointing_constrain_dframe['hourangle_west'][m])

		mjd = []
		ut_time = []
		lst = []
		zenith = []
		hour_angle = []
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
			# print(observatory.date,str(observatory.sidereal_time()))


			# calculate altitude angle or zenith angular distance of the sun   ---------------------------------
			solar = ephem.Sun(observatory)
			solar_alt_dd = 90 - float(str(solar.alt).split(":")[0])+float(str(solar.alt).split(":")[1])/60.0+float(str(solar.alt).split(":")[2])/3600.0
			#print('solar  %s' % (solar_alt_dd))
			

			lunar = ephem.Moon(observatory)
			lunar_ra_dd = float(str(lunar.ra).split(":")[0])* 15.0+float(str(lunar.ra).split(":")[1])/60.0* 15.0+float(str(lunar.ra).split(":")[2])/3600.0* 15.0
			lunar_dec_dd = float(str(lunar.dec).split(":")[0])+float(str(lunar.dec).split(":")[1])/60.0+float(str(lunar.dec).split(":")[2])/3600.0
			#print('lunar %s %s %s' % (lunar_ra_dd, lunar_dec_dd, lunar.moon_phase))

			# calculate zenith angular distance of field center and all vertexes
			zenith_ang_dis_cen_dd = (angular_distance(racen[i], deccen[i],lst_dd,lat_dd))

			# calculate angular distance between field center and all vertexes and moon
			moon_ang_dis_min = (angular_distance(racen[i], deccen[i],lunar_ra_dd,lunar_dec_dd))

			# set mini distance from the moon
			for mm in range(len(moon_dis_phase_data)):
				if (lunar.moon_phase >= moon_dis_phase_data[mm][0] and lunar.moon_phase < moon_dis_phase_data[mm][1]):
					moon_dis_min = moon_dis_phase_data[mm][2]
					break

			# calculate hour angle
			hour_angle_n = ((lst_dd - racen[i]) / 15.0)
			# print(lst_dd,hour_angle_n)
			if hour_angle_n >= 12.0 and hour_angle_n <= 24.0:
				hour_angle_n = hour_angle_n - 24.0
			if hour_angle_n <= -12.0 and hour_angle_n >= -24.0:
				hour_angle_n = hour_angle_n + 24.0

			if ( solar_alt_dd > zenith_sun_min ) and ( zenith_ang_dis_cen_dd < zenith_min ) and (moon_ang_dis_min > moon_dis_min ) and (hour_angle_n >= hourangle_east) and (hour_angle_n <= hourangle_west): # and (galactic_lat_min[0] > gal_min) and (moon_ang_dis_min > moon_dis_min ):		
				mjd.append(MJD_time)
				ut_time.append(UT_nighttime_current_str_T)
				lst.append(lst_dd)
				zenith.append(zenith_ang_dis_cen_dd)
				hour_angle.append(hour_angle_n)


		obs_cons = 0
		mjd_begin = 0.0
		mjd_end = 0.0
		obs_phase = ''
		if (len(mjd) > 0 ):
			obs_mjd_begin_index = 0
			obs_mjd_end_index = 0
			if obs_mjd_begin_index == 0:
				obs_mjd_begin_index = mjd.index(min(mjd))
				obs_mjd_end_index = mjd.index(max(mjd))
				mjd_begin = mjd[obs_mjd_begin_index]
				mjd_end = mjd[obs_mjd_end_index]
				lst_begin = lst[obs_mjd_begin_index]
				lst_end = lst[obs_mjd_end_index]
				zenith_begin = zenith[obs_mjd_begin_index]
				zenith_end = zenith[obs_mjd_end_index]
				date_begin = jd2gcal(2400000.5, mjd_begin) #`` float
				date_end = jd2gcal(2400000.5, mjd_end) #`` flaot
				begin_hh = int(date_begin[3] * 24.0 ) #``
				begin_mm = int((date_begin[3] * 24.0 - begin_hh)*60.0) #``                          The last element of the tuple is the same as
				begin_ss = (((date_begin[3] * 24.0 - begin_hh)*60.0) - begin_mm)*60.0 #``              (hh + mm / 60.0 + ss / 3600.0) / 24.0  
				end_hh = int(date_end[3] * 24.0 ) #``                                               where hh, mm, and ss are the hour, minute and second of the day.
				end_mm = int((date_end[3] * 24.0 - end_hh)*60.0) #``
				end_ss = (((date_end[3] * 24.0 - end_hh)*60.0) - end_mm)*60.0 #``
				calendar_date_begin = "%d/%02d/%02d %02d:%02d:%02d" % (date_begin[0],date_begin[1],date_begin[2],begin_hh,begin_mm,begin_ss) #``
				calendar_date_end = "%d/%02d/%02d %02d:%02d:%02d" % (date_end[0],date_end[1],date_end[2],end_hh,end_mm,end_ss) #``
				obs_stag = 'observable'
				# obs_phase = "continous_slot"
	
		if mjd_begin >= mjd_end:
			mjd_begin = 0.0
			mjd_end = 0.0
			calendar_date_begin = "0000/00/00 00:00:00"
			calendar_date_end = "0000/00/00 00:00:00"
			obs_stag = 'unobservable'

		if priority[i] >= 90 and priority[i] <=99:
			obs_stag = 'observable'
			
		data.add_row([obj_id[i],calendar_date_begin,calendar_date_end,obs_stag])

	return data


