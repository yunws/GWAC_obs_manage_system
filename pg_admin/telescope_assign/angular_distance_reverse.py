# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""
import math
import sys
import numpy as np

## {{{ http://code.activestate.com/recipes/52224/ (r1)

#def hav(x):
#    hav_result = (1-math.cos(x))/2
#    return hav_result

def hav(x):
    hav_result = math.sin(x/2)*math.sin(x/2)
    return hav_result

def angular_distance_reverse(alpha1_d,beta1_d,beta2_d,d_deg):
    ind = 3.1415926 / 180.0
    alpha1_radian=alpha1_d  * ind
    beta1_radian=beta1_d * ind
    beta2_radian=beta2_d * ind
    
    d_radian = d_deg * ind
    hav_d  = (1 - math.cos(d_radian))/2
    delta_beta = abs(beta1_radian - beta2_radian)
    hav_delta_beta = math.sin(delta_beta/2.)*math.sin(delta_beta/2.) 
    
    hav_delta_alpha = ( (1 - math.cos(d_radian))/2 - hav_delta_beta) / (math.cos(beta1_radian)*math.cos(beta2_radian))
    # print 'hav_delta_alpha',hav_delta_alpha
    if hav_delta_alpha >= 0 and hav_delta_alpha <= 1:
        sqrt_hav_delta_alpha = math.sqrt(hav_delta_alpha)
        delta_alpha = math.asin(sqrt_hav_delta_alpha) * 2.
        alpha2_radian_nag = alpha1_radian - delta_alpha
        alpha2_radian_pos = alpha1_radian + delta_alpha
        alpha2_d_nag = alpha2_radian_nag / ind
        alpha2_d_pos = alpha2_radian_pos / ind
    elif hav_delta_alpha >= -0.001 and hav_delta_alpha < 0.0:
        hav_delta_alpha = 0
        sqrt_hav_delta_alpha = math.sqrt(hav_delta_alpha)
        delta_alpha = math.asin(sqrt_hav_delta_alpha) * 2.
        alpha2_radian_nag = alpha1_radian - delta_alpha
        alpha2_radian_pos = alpha1_radian + delta_alpha
        alpha2_d_nag = alpha2_radian_nag / ind
        alpha2_d_pos = alpha2_radian_pos / ind
    else:
        print('Value Error: math domain error')
        alpha2_d_nag = 0
        alpha2_d_pos = 0

    if alpha2_d_nag < 0:
        alpha2_d_nag = 360 + alpha2_d_nag
    return alpha2_d_nag,alpha2_d_pos






