#-*- coding:utf-8 -*-
from get_observation_info import *
import time
import datetime
from get_trigger_name_by_id import get_trigger_name_by_id
from get_gwac_followup_observation_log import get_gwac_trigger_info
from get_gwac_followup_observation_log import Retrieve_trigger_name

time_now = (datetime.datetime.now() + datetime.timedelta(hours= -24)).strftime("%Y/%m/%d")
print(time_now)
time_test = '2020/01/14'
pp = check_trigger(time_now)
ss = []
# ss.append(get_gwac_trigger_info())
zxc = get_gwac_trigger_info()
# print(zxc)
zzz = []
for i in zxc:
    uu = tuple(i.split(' '))
    # print(uu)
    zzz.append(uu)
# print(zzz)
ss.append(zzz)
ss.append(pp[1])
ss.append(pp[2])
print(ss)
# print(len(ss))


trigger_name_dict = {}
obj_sour_all = []
obj_source = []
trigger_name = []


check_result_trigger = check_trigger(time_test)


for i in check_result_trigger:
    for aa in i:
        obj_source_pre = aa[8]
        if obj_source_pre not in obj_source:
            obj_source.append(obj_source_pre)

for i in obj_source:
    obj_sour = i.split('_')[-2]
    if obj_sour not in obj_sour_all:
        obj_sour_all.append(obj_sour)


for obj_sour_trigger in obj_sour_all:
    trigger_name_pre  = get_trigger_name_by_id('xinglong', obj_sour_trigger)
    # print(trigger_name_pre)
    trigger_name_dict[obj_sour_trigger] = trigger_name_pre

for i in trigger_name_dict.values():
    if i not in trigger_name:
        trigger_name.append(i)

# print(obj_sour_all)
# print(trigger_name)
# print(obj_source)
# print(trigger_name_dict)



with open('GWAC_observation_log.txt', 'w+') as f:
    info = '#data: {zz_time_now} \n#======================== \n# ToO observations: \n#++++++++++++++++++++++++ \n' \
    .format(zz_time_now = time_now)
    f.write(info +'\n')
    tel = ['\n\n#GWAC observation log: \nI,op_sn, Group_ID, Unit_ID, Grid_ID, Field_ID, RA, DEC, trigger_name, B_UT , E_UT, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE, FoV, Limit Mag, obj_sour, obj_name, obstype, priority\n', \
    '\n\n#F60 observation log: \nobj_id, op_sn, Group_ID, Unit_ID, RA, DEC, B_UT, E_UT, obj_sour, obj_name, obstype, priority, obs_stage, image_path\n',\
    '\n\n#F30 observation log: \nobj_id, op_sn, Group_ID, Unit_ID, RA, DEC, B_UT, E_UT, obj_sour, obj_name, obstype, priority, obs_stage, image_path\n']

    if trigger_name:
        for a in trigger_name:
            name = a 
            f.write('Trigger name :' + name + '\n')

            idd = filter(lambda x:a == x[1], trigger_name_dict.items())
            idd_tri = []
            for (key,value) in idd:
                if key not in idd_tri:
                    idd_tri.append(key)
            f.write('Trigger ID :' + str(idd_tri) + '\n')

            ojbsource = []
            for i in obj_source:
                if i.split('_')[-2] in idd_tri:
                    if i not in ojbsource:
                        ojbsource.append(i)
            f.write('Objsource :' + str(ojbsource) + '\n')

            for i in range(3):
                f.write(tel[i] + '\n')
                for k in ss[i]:
                    kk = []
                    for kkk in k:
                        if kkk in ojbsource:
                            kk.append(str(k))
                    kk = ', '.join(kk)
                    f.write(kk + '\n')

    tel = ['\n\n#GWAC observation log: \nobj_id, op_sn, Group_ID, Unit_ID, Grid_ID, Field_ID, RA, DEC, obj_sour, obj_name, obstype, priority, obs_stage\n', \
    '\n\n#F60 observation log: \nobj_id, op_sn, Group_ID, Unit_ID, RA, DEC, B_UT, E_UT, obj_sour, obj_name, obstype, priority, obs_stage, image_path\n',\
    '\n\n#F30 observation log: \nobj_id, op_sn, Group_ID, Unit_ID, RA, DEC, B_UT, E_UT, obj_sour, obj_name, obstype, priority, obs_stage, image_path\n']
    yy = check_routine_observation(time_now)
    info = '\n\n\n#======================== \n# Routine observations: \n#++++++++++++++++++++++++ \n'
    f.write(info +'\n')
    for i in range(3):
        f.write(tel[i] +'\n')
        for k in yy[i]:
            kk = []
            for kkk in k:
                kk.append(str(kkk))
            kk = ', '.join(kk)
            f.write(kk +'\n')