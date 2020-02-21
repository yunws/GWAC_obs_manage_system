# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""
__author__='Xuhui Han'
__version__ ='$Revision: 1.3 $'
import os
import time
import datetime
from confi_field_tele_constrain import confi_field_tele_constrain
from astro_parameter_calculate import astro_parameter_calculate

def func_field_tele_constrain(tele_pointing_constrain_dframe,Group_ID,Unit_ID,racen, deccen,lst_begin,lst_end,current_utc_datetime):	

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
			moon_dis_phase_data = [[]]
			for moon_dis_para in moon_dis_para_str:
				moon_dis_para_phase_min = float(moon_dis_para.split(':')[0].split('-')[0])
				moon_dis_para_phase_max = float(moon_dis_para.split(':')[0].split('-')[1])
				moon_dis_para_dis = float(moon_dis_para.split(':')[1])
				moon_dis_phase_data.append([moon_dis_para_phase_min,moon_dis_para_phase_max,moon_dis_para_dis])
			moon_dis_phase_data = filter(None,moon_dis_phase_data)

	# set observatory parameters ----------------------------------------
	# current_utc_datetime = time.gmtime()
	current_utc_datetime_str_T = time.strftime('%Y/%m/%dT%H:%M:%S', current_utc_datetime)
	# current_utc_datetime_str = time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime())

	# current_utc_datetime_str = "2017/11/20 15:50:00"
	# current_utc_datetime_str_T = "2017/11/20T15:50:00"
	# current_utc_datetime = time.strptime(current_utc_datetime_str, '%Y/%m/%d %H:%M:%S')
	# day_string = time.strftime('%Y/%m/%d',current_utc_datetime)
	
	# print current_utc_datetime
	astro_parameter = astro_parameter_calculate(current_utc_datetime)


	lat_dd = astro_parameter[0]
	locat_time = astro_parameter[1]
	current_lst_dd = astro_parameter[2]
	solar_alt_dd = astro_parameter[3]
	lunar_ra_dd = astro_parameter[4]
	lunar_dec_dd = astro_parameter[5]
	lst_ut_dd = astro_parameter[6]
	obs_able = 0
	obs_cons = 0


	hour_ang_current = (current_lst_dd - racen) / 15.0
	if hour_ang_current > 12:
		hour_ang_current = hour_ang_current - 24
	elif hour_ang_current < (-12):
		hour_ang_current = 24 + hour_ang_current

	hour_ang_west = ( current_lst_dd - lst_begin) / 15.0

	if hour_ang_west > 12:
		hour_ang_west = hour_ang_west - 24
	elif hour_ang_west < (-12):
		hour_ang_west = 24 + hour_ang_west

	hour_ang_east = ( current_lst_dd - lst_end ) / 15.0
	if hour_ang_east > 12:
		hour_ang_east = hour_ang_east - 24
	elif hour_ang_east < (-12):
		hour_ang_east = 24 + hour_ang_east

	# print 'lst ',current_lst_dd,racen,'lst_begin',lst_begin,lst_end
	# print 'lst dd',(racen - current_lst_dd )/15,( lst_begin - current_lst_dd)/15,( lst_end - current_lst_dd )/15
	# print 'hour ang',hour_ang_current,hour_ang_west,hour_ang_east
	# tele_pointing_constrain_dframe = confi_field_tele_constrain('file',Group_ID,Unit_ID)

	# datetime_current_utc_datetime = datetime.datetime.strptime(current_utc_datetime_str_T, '%Y/%m/%dT%H:%M:%S')
	# utc_datetime_begin = datetime_current_utc_datetime + datetime.timedelta(hours=hour_ang_east)
	# utc_datetime_begin_str_T = utc_datetime_begin.strftime('%Y/%m/%dT%H:%M:%S')		
	# utc_datetime_end = datetime_current_utc_datetime + datetime.timedelta(hours=hour_ang_west)
	# utc_datetime_end_str_T = utc_datetime_end.strftime('%Y/%m/%dT%H:%M:%S')
	# print(current_lst_dd,racen, deccen,utc_datetime_begin_str_T,current_utc_datetime_str_T,utc_datetime_end_str_T)
	
	obs_cons = 0
	# print 'test1',obs_cons,hour_ang_west,hour_ang_east
	for n in range(len(tele_pointing_constrain_dframe)):
		hourangle_east = ( tele_pointing_constrain_dframe['hourangle_east'][n])
		hourangle_west = (tele_pointing_constrain_dframe['hourangle_west'][n])
		if deccen >= tele_pointing_constrain_dframe['dec_deg'][n] and deccen < (tele_pointing_constrain_dframe['dec_deg'][n] + 10 ) and \
		hour_ang_current > hourangle_east and hour_ang_current < hourangle_west:
			if hour_ang_east < hourangle_east:
				hour_ang_east = hourangle_east
			if hour_ang_west > hourangle_west:
				hour_ang_west = hourangle_west
			datetime_current_utc_datetime = datetime.datetime.strptime(current_utc_datetime_str_T, '%Y/%m/%dT%H:%M:%S')
			utc_datetime_begin = datetime_current_utc_datetime + datetime.timedelta(hours=hour_ang_east)
			utc_datetime_begin_str_T = utc_datetime_begin.strftime('%Y/%m/%dT%H:%M:%S')		
			utc_datetime_end = datetime_current_utc_datetime + datetime.timedelta(hours=hour_ang_west)
			utc_datetime_end_str_T = utc_datetime_end.strftime('%Y/%m/%dT%H:%M:%S')
			if (hour_ang_west >= hour_ang_east):
				obs_cons = 1
				# print 'test2',obs_cons,n,tele_pointing_constrain_dframe['dec_deg'][n],obs_cons,hour_ang_west,hour_ang_east
				# print 'time',current_utc_datetime_str_T,utc_datetime_begin_str_T,utc_datetime_end_str_T
				# print 'hour angle',(datetime_current_utc_datetime + datetime.timedelta(hours=0.775450000022))
				break

	if obs_cons == 0:
		utc_datetime_begin_str_T = "0000/00/00T00:00:00"
		utc_datetime_end_str_T = "0000/00/00T00:00:00"

	# print obs_cons

	return [obs_cons,utc_datetime_begin_str_T,utc_datetime_end_str_T]


# func_field_tele_constrain("GWAC_XL1",'004',350.11,70.50, 338,114)
