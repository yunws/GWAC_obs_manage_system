# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""
__author__='Xuhui Han'
__version__ ='$Revision: 1.2 $'
import os
import pandas as pd


def confi_field_tele_constrain(dataio,Group_ID,Unit_ID_selected):	
	# if dataio == "database":
	# 	import MySQLdb

	# 	homedir = os.getcwd()
	# 	configuration_file = './configuration_xl.dat'
	# 	configuration_file_dev = open(configuration_file,'rU')

	# 	lines1=configuration_file_dev.read().splitlines()
	# 	configuration_file_dev.close()

	# 	for line1 in lines1:
	# 	    word=line1.split()
	# 	    if word[0] == 'griduser':
	# 	        griduser = word[2]
	# 	    elif word[0] == 'gridip':
	# 	        gridip = word[2]
	# 	    elif word[0] == 'gridmypassword':
	# 	        gridmypassword = word[2]
	# 	    elif word[0] == 'gridmydb':
	# 	        gridmydb = word[2]
	# 	    elif word[0] == 'cataip':
	# 	        cataip = word[2]
	# 	    elif word[0] == 'catauser':
	# 	        catauser = word[2]
	# 	    elif word[0] == 'catamypassword':
	# 	        catamypassword = word[2]
	# 	    elif word[0] == 'catamydb':
	# 	        catamydb = word[2]    

	# 	# connect gwacoc database  -----------------------------------------
	# 	conn_gwacoc_cmd_loading = MySQLdb.connect(host = gridip,
	# 	                user = griduser,
	# 	                passwd = gridmypassword,
	# 	                db =  gridmydb) 

	# 	# read telescope pointing constrain data -----------------------------------------
	# 	tele_point_table = "tele_pointing_constrain"
	# 	cursor_tele_pointing_constrain = conn_gwacoc_cmd_loading.cursor () 
	# 	tele_point_cmd = "select * from " + tele_point_table + " where Group_ID = '" + Group_ID + "' and Unit_ID = '" + Unit_ID_selected + "'"
	# 	cursor_tele_pointing_constrain.execute(tele_point_cmd)
	# 	extract_tele_pointing_constrain_result = cursor_tele_pointing_constrain.fetchall()         
	# 	cursor_tele_pointing_constrain.close()
	# 	conn_gwacoc_cmd_loading.close()
	# 	obs_cons = 0

	# 	# define telescope pointing constarin data frame for each telescope----------------------------------------
	# 	tele_pointing_constrain_dframe = pd.DataFrame()
	# 	if len(extract_tele_pointing_constrain_result) > 0:
	# 		tele_pointing_constrain_dframe['Group_ID'] = zip(*extract_tele_pointing_constrain_result)[1]
	# 		tele_pointing_constrain_dframe['Unit_ID'] = zip(*extract_tele_pointing_constrain_result)[2]
	# 		tele_pointing_constrain_dframe['dec_deg']  = zip(*extract_tele_pointing_constrain_result)[3]
	# 		tele_pointing_constrain_dframe['hourangle_east'] = zip(*extract_tele_pointing_constrain_result)[4]
	# 		tele_pointing_constrain_dframe['hourangle_west'] = zip(*extract_tele_pointing_constrain_result)[5]
	
	# elif dataio == "file":

		# read telescope pointing constrain data -----------------------------------------
		tele_point_table = "tele_pointing_constrain.csv"
		tele_pointing_constrain_dframe_all = pd.read_csv(tele_point_table)
		tele_pointing_constrain_dframe =  tele_pointing_constrain_dframe_all[(tele_pointing_constrain_dframe_all["Unit_ID"] == int(Unit_ID_selected))].copy().reset_index(drop=True)
	else:
		print("Wrong data")

	return tele_pointing_constrain_dframe

# 
# confi_field_tele_constrain('file','GWAC_XL1','004')