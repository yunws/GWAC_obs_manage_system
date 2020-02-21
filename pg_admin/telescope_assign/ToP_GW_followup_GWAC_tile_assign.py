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


def ToP_GW_followup_GWAC_tile_assign(Group_IDs,Unit_IDs,Grid_IDs,field_IDs,Pointing_RAs,Pointing_DECs,Obj_Ranks):
	group_num = len(Group_IDs)
	unit_num = len(Unit_IDs)
	field_num = len(field_IDs)
	print('num:',group_num,unit_num,field_num)
	print('fields:',Grid_IDs,field_IDs)


	return 