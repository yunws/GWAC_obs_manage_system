#coding=utf-8

# from __future__ import unicode_literals
import os
import sys
import time
import datetime
import codecs
import configparser
#from communication_client import Client
#from object_generator import obj_insert

#client = Client('object_generator')

if not sys.argv[1:]:
    obj_file = 'obj.conf'
else:
    obj_json = sys.argv[1]
#print obj_json
conf = configparser.ConfigParser()
conf.read(obj_file, encoding="utf-8")
while True:
    print('\n\nItems: ')
    items = conf.items('Object')
    for i in range(len(items)):
        i += 1
        #print('%s: %s ==> %s ' % (str(i), items[i-1][0], items[i-1][1]))
        print("{:^3}:{:^15}{:^5}{:^10}".format(str(i), items[i-1][0], "==>", items[i-1][1]))
    print('\nPlease input the item number which you want to modify (Example: 1 Test), type "Enter" to Go.')
    k_in = input(':')
    k_in = k_in.strip()
    if k_in:
        try:
            k_in = k_in.split(',')
        except:
            k_in = [k_in]
        for k_in_item in k_in:
            try:
                k_in_item = k_in_item.strip()
                if k_in_item:
                    k_in_item = k_in_item.split()
                    if k_in_item[0].isdigit() and k_in_item[1].isalnum:
                        conf.set('Object',items[int(k_in_item[0])-1][0], k_in_item[1])
            except:
                print('Something is Wrong.')
                break
    else:
        break
conf.write(open(obj_file, "w"))

date_indb = datetime.datetime.utcnow().strftime("%Y/%m/%d")
items = dict(conf.items('Object'))
#print(items)
if int(items['runs']) == -1:
    read_time = items['read_time']
    expdur_run = items['expdurs'].strip().split('/')
    expdur_tol = 0.0
    for k in expdur_run:
        expdur_tol = expdur_tol + float(read_time) + float(k)
    #print(expdur_tol)
    runs = int(float(items['block_time'])*60/(expdur_tol+float(items['run_dely'])))
else:
    runs = items['runs']
print('\nRuns: %s' % str(runs))
blocks = int(items['blocks'])
block_dely = float(items['block_dely'])
if float(items['begin_time']) > 0:
    print('\nIt is waiting...')
    time.sleep(float(items['begin_time'])*60*60)
for k in range(blocks):
    print('\nBlok now: %s' % str(k+1))
    obj_line = "obj_name=%s objsour=Block observer=%s objra=%s objdec=%s objepoch=2000 objerror=0.0|0.0 objrank=0 group_id=%s unit_id=%s obs_type=oba obs_stra=pointing date_beg=%s date_end=%s day_int=0 imgtype=object filter=%s expdur=%s frmcnt=%s priority=%s run_name=%s dely=%s note=%s mode=observation\n" % \
        (items['obj_name'], 'GWAC', items['ra'], items['dec'], items['group_id'], items['unit_id'], date_indb, date_indb, items['filters'], items['expdurs'], items['frmcnts'], items['priority'], runs, items['run_dely'], items['filter_dely'])
    print(obj_line)
    #obj_insert(obj_line)
    #client.Send()
    if blocks > 1:
        time.sleep(block_dely*60)
print('\nDone.')
