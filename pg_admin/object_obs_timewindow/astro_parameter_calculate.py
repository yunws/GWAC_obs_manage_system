import os
import ephem
import time
from time import gmtime, strftime,localtime
def astro_parameter_calculate(utc_datetime):


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
			moon_dis_phase_data = [[]]
			for moon_dis_para in moon_dis_para_str:
				moon_dis_para_phase_min = float(moon_dis_para.split(':')[0].split('-')[0])
				moon_dis_para_phase_max = float(moon_dis_para.split(':')[0].split('-')[1])
				moon_dis_para_dis = float(moon_dis_para.split(':')[1])
				moon_dis_phase_data.append([moon_dis_para_phase_min,moon_dis_para_phase_max,moon_dis_para_dis])
			moon_dis_phase_data = filter(None,moon_dis_phase_data)

	# set observatory parameters ----------------------------------------
	observatory = ephem.Observer()
	observatory.lat = observatory_lat
	observatory.lon = observatory_lon
	observatory.elevation = observatory_elevation
	lat_dd = float(str(observatory.lat).split(":")[0])+\
	float(str(observatory.lat).split(":")[1])/60.0+\
	float(str(observatory.lat).split(":")[2])/3600.0

	# print "utc_datetime", utc_datetime
	utc_datetime_str = time.strftime('%Y/%m/%d %H:%M:%S', utc_datetime)
	# print "utc_datetime_str",utc_datetime_str
	observatory.date = utc_datetime_str

	# calculate local time  ----------------------------------------
	local_time = ephem.localtime(observatory.date)
	local_time_str = str(local_time).replace(' ','T')

	# calculate local sidereal time  ----------------------------------------
	lst_dd = float(str(observatory.sidereal_time()).split(":")[0])* 15.0+\
	float(str(observatory.sidereal_time()).split(":")[1])/60.0* 15.0+\
	float(str(observatory.sidereal_time()).split(":")[2])/3600.0* 15.0

	# calculate time difference between ut and lst ----------------------------------
	ut_time_dd = float(str(observatory.date).split(" ")[1].split(":")[0])* 15.0+\
	float(str(observatory.date).split(" ")[1].split(":")[1])/60.0* 15.0+\
	float(str(observatory.date).split(" ")[1].split(":")[2])/3600.0* 15.0
	lst_ut_dd = lst_dd - ut_time_dd
	# print 'lst_ut', observatory.date, local_time,observatory.sidereal_time(), lst_dd,ut_time_dd,lst_ut_dd

	# calculate altitude angle or zenith angular distance of the sun   ---------------------------------
	solar = ephem.Sun(observatory)
	solar_alt_dd = 90 - float(str(solar.alt).split(":")[0])+float(str(solar.alt).split(":")[1])/60.0+float(str(solar.alt).split(":")[2])/3600.0
	#print('solar  %s' % (solar_alt_dd))

	lunar = ephem.Moon(observatory)
	lunar_ra_dd = float(str(lunar.ra).split(":")[0])* 15.0+float(str(lunar.ra).split(":")[1])/60.0* 15.0+float(str(lunar.ra).split(":")[2])/3600.0* 15.0
	lunar_dec_dd = float(str(lunar.dec).split(":")[0])+float(str(lunar.dec).split(":")[1])/60.0+float(str(lunar.dec).split(":")[2])/3600.0
	moon_phase = float(lunar.moon_phase)
	#print('lunar %s %s %s' % (lunar_ra_dd, lunar_dec_dd, lunar.moon_phase))

	return [lat_dd,local_time_str,lst_dd,solar_alt_dd,lunar_ra_dd,lunar_dec_dd,lst_ut_dd, moon_dis_phase_data,moon_phase,zenith_sun_min,zenith_min,gal_min]




