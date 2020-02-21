# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""

import sys, stat
import math
import subprocess as subp
## {{{ http://code.activestate.com/recipes/52224/ (r1)
from os.path import exists, join
import os.path
from os import pathsep
import string
import os
import time
from time import gmtime, strftime,localtime
# from db_telegwac_obs_cmd_loading import db_telegwac_obs_cmd_loading


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def ToP_GW_followup_GWAC_tile_assign(Group_ID,Unit_ID,Grid_ID,field_ID,Pointing_RA,Pointing_DEC,Obj_Rank):

	mode = mode_para[0]

	current_utc_datetime = time.gmtime()
	Op_time = time.strftime('%Y-%m-%dT%H:%M:%S', gmtime())
	Op_type = 'obs'
	gwac_append_cmd = "append_gwac"
	Observation_Type = 'toa'
	Object_ID = obs_id
	epoch = '2000'
	ObjRA = Pointing_RA
	ObjDEC = Pointing_DEC
	ObjEpoch = epoch
	ObjError = '0.0|0.0'
	ImgType = 'object'
	expdur = '10'
	delay = '5'
	frmcnt = '-1'
	priority = '65'
	Pair_ID = '0' 
	begin_time = "0000-00-00T00:00:00"
	end_time = "0000-00-00T00:00:00"

	db_telegwac_obs_cmd_loading(mode_para,gwac_append_cmd,Op_time, Op_type, Group_ID, Unit_ID, Observation_Type, \
	Grid_ID, field_ID, Object_ID, Pointing_RA, Pointing_DEC, epoch, ObjRA, ObjDEC, ObjEpoch, ObjError,\
	ImgType, expdur, delay, frmcnt, priority, begin_time,end_time,Pair_ID,mode)

	return 