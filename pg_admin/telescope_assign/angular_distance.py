# -*- coding: utf-8 -*-
"""
Created on Sun Feb 05 10:46:24 2012

@author: han
"""
import math
## {{{ http://code.activestate.com/recipes/52224/ (r1)

#def hav(x):
#    hav_result = (1-math.cos(x))/2
#    return hav_result

def hav(x):
    hav_result = math.sin(x/2)*math.sin(x/2)
    return hav_result

alpha1_d=90.0
beta1_d=48.2
alpha2_d=90.5
beta2_d=49.3

def angular_distance(alpha1_d,beta1_d,alpha2_d,beta2_d):

    ind = 3.1415926 / 180.0
    #ind = 1
    alpha1_radian=alpha1_d  * ind
    beta1_radian=beta1_d * ind
    alpha2_radian=alpha2_d * ind 
    beta2_radian=beta2_d * ind
    
    delta_alpha = alpha1_radian - alpha2_radian
    delta_beta = beta1_radian - beta2_radian
    hav_delta_alpha = hav(delta_alpha)
    hav_delta_beta = hav(delta_beta)
    hav_d = hav_delta_beta + math.cos(beta1_radian)*math.cos(beta2_radian)*hav_delta_alpha
    d_radian = math.acos(1 - (hav_d *2))
    d_deg = d_radian / ind
#    print d_deg
    return d_deg

