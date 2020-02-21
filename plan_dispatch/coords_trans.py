#!/usr/bin/python
# -*- coding: utf-8 -*-
#author: Xiao Yujie
#update: 2018-12-19

def Ra_to_degree(i):
    if ':' in i:
        RaList = map(float,i.split(':'))
        return str(RaList[0]*15 + RaList[1]/4 + RaList[2]/240)
    elif abs(float(i)) <= 360:
        return str(i)
    else:
        print 'Wrong RA, please input hms or degree'

def Dec_to_degree(i):
    if ':' in i:
        if '-' not in i:
            DecList = map(float,i.split(':'))
            return str(DecList[0] + DecList[1]/60 + DecList[2]/3600)
        else:
            DecList = map(float,i.split(':'))
            return str(DecList[0] - DecList[1]/60 - DecList[2]/3600)
    elif abs(float(i)) <= 360:
        return str(i)
    else:
        print 'Wrong DEC, please input dms or degree'

def Ra_to_h(i):
    if ':' in i:
        RaList = map(float,i.split(':'))
        return str(RaList[0] + RaList[1]/60 + RaList[2]/3600)
    elif abs(float(i)) <= 360:
        a = float(i)/15
        return str(a)
    else:
        print 'Wrong RA, please input dms or degree'

def Ra_to_hms(i):
    if ':' in i:
        RaList = map(float, i.split(':'))
        if RaList[0] >= 0 and RaList[0] < 24 and RaList[1] < 60 and RaList[2] < 60:
            return i
    elif abs(float(i)) < 360:
        a = float(i)/15
        b = (float(i)-15*int(a))*4
        c = (b-int(b))*60
        if len(str(int(c))) > 1:
            return '%02d:%02d:%.1f' % (a,b,c)
        else:
            return '%02d:%02d:0%.1f' % (a,b,c)
    else:
        print 'Wrong RA, please input hms or degree'

def Dec_to_dms(i):
    if ':' in i:
        DecList = map(float, i.split(':'))
        if abs(DecList[0]) < 90 and DecList[1]/60 < 60 and DecList[2]/3600 < 60:
            return i
    elif abs(float(i)) < 90:
        i = float(i)
        a = (i-int(i))*60
        b = (a-int(a))*60
        if i >= 0:
            return '+%02d:%02d:%02d' % (int(i), abs(a), abs(b))

        if i < 0:
            if int(i) == 0:
                return '-%02d:%02d:%02d' % (int(i), abs(a), abs(b))
            else:
                return '%02d:%02d:%02d' % (int(i), abs(a), abs(b))
    else:
        print 'Wrong DEC, please input dms or degree'