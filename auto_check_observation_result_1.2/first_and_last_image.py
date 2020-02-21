#!/usr/bin/python
# -*- coding: utf-8 -*-
#@author: Xuhui Han
import datetime
import time

def first_and_last_image(data):
    mark_list = []
    all_list = []  
    for n_data in range(len(data[0])):
        i = [data[0][n_data],data[1][n_data],data[2][n_data],data[3][n_data],data[4][n_data],data[5][n_data],data[6][n_data],data[7][n_data],data[9][n_data],data[10][n_data],data[11][n_data]]
        if i[1:4] not in mark_list:
            mark_list.append(i[1:4])
            status_dict = dict(B_UT = i[0],E_UT = i[0],Image_RA = i[4],Image_DEC = i[5],CCD_ID = i[6],CCD_TYPE = i[7], FWHM = i[8], S2N = i[9], LIMIT_MAG = i[10])
            all_list.append(status_dict)
            # print 'add new'
        else:
            m = mark_list.index(i[1:4])
            # print i[1:4],m,'all_list',all_list[m]['B_UT']
            if i[0] < all_list[m]['B_UT']:
                all_list[m]['B_UT'] = i[0]
            if i[0] > all_list[m]['E_UT']:
                all_list[m]['E_UT'] = i[0]
            # print 'change record'
        # print n_data,all_list
    return all_list,mark_list