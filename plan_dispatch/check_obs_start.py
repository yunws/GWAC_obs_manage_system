#coding=utf-8

from __future__ import unicode_literals
import re
import sys
import datetime
import subprocess
import psycopg2
from pydash import at

def Ra_to_h(ra):
    if ':' in ra:
        RaList = map(float,ra.split(':'))
        return str(RaList[0] + RaList[1]/60 + RaList[2]/3600)
    elif abs(float(ra)) <= 360:
        a = float(ra)/15
        return str(a)
    else:
        exit('\n\033[0;31mWARNING:\033[0m Wrong RA.')

def send_obj(obj_name, ra, dec, filter, expdur, frmcnt, priority):
    ra = Ra_to_h(ra)
    cmd_in = "append_plan %s %s && append_object %s %s %s 2000 4 %s %s %s %s && append_plan gwac default" % ("GWAC", "oba", obj_name, ra, dec, expdur, frmcnt, filter, priority)
    cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #res = cmd.stdout.read()
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return time_now

def check_obj_status(obj_name, filter, frmcnt, pre_time):
    time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
    date_now = datetime.datetime.utcnow().strftime("%y%m%d")
    count = 0
    com_mark = 0
    ###
    cmd_in = 'ls /tmp/camagent*.log | sort -r'
    cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logs = cmd.stdout.read()
    if logs:
        for log in logs:
            log = log.strip()
            cmd_in = "cat " + log + " | grep 'Image is saved as " + obj_name + ".*_" + filter + "_" + date_now+ "_.*.fit'"
            cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            items = cmd.stdout.read()
            if items:
                for item in items:
                    item = item.strip()
                    item_com_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                    if item_com_time >= pre_time:
                        #print '\n'+item
                        count += 1
                    if count == int(frmcnt):
                        log_com_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                        com_mark = 1
                        break
                if com_mark == 1:
                    break
            if com_mark == 0:
                cmd_in = "tail " + log + ' | grep "20[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*" | sort -r'
                cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                res = cmd.stdout.read()
                if res:
                    res = res[0].strip()
                    re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res)
                    if re_time and re_time.group(0) <= time_limit:
                        break
                cmd_in = "tac " + log + " | grep 'New object(Name, RA, DEC):  %s.*'" % obj_name
                cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                res = cmd.stdout.read()
                if res:
                    n_it = res[0].strip()
                    n_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", n_it).group(0)
                    if n_time >= pre_time:
                        log_dist_time = n_time
                        cmd_in = "tac " + log + " | grep 'ERROR: Filter input error'"
                        cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        res = cmd.stdout.read()
                        if res:
                            for item in res:
                                item = item.strip()
                                item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                if item_time >= log_dist_time:
                                    print '\n' + item
                                    log_com_time = item_time
                                    com_mark = 2
                                    break
                            if com_mark == 2:
                                break
                    else:
                        cmd_in = "tac " + log + " | grep 'WARN: Trying to change.*'"
                        cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        res = cmd.stdout.read()
                        if res:
                            for item in res:
                                item = item.strip()
                                item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                if item_time >= pre_time:
                                    print '\n' + item
                                    log_com_time = item_time
                                    com_mark = 2
                                    break
                            if com_mark == 2:
                                break
                else:
                    cmd_in = "tac " + log + " | grep 'WARN: Trying to change.*'"
                    cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    res = cmd.stdout.read()
                    if res:
                        for item in res:
                            item = item.strip()
                            item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                            if item_time >= pre_time:
                                print '\n' + item
                                log_com_time = item_time
                                com_mark = 2
                                break
                        if com_mark == 2:
                            break
    else:
        print "\n\033[0;31mWARNING:\033[0m There is no camagent log!"
        return 0
    if com_mark == 2:
        return [2, log_com_time]
    elif com_mark == 1:
        return [1, log_com_time]
    else:
        return 0
        # proc = '%.0f%%' % ((count/float(frmcnt))*100)
        # return [0, proc]

def check_status(obj_name, ra, dec, filters, expdurs, frmcnts, runs, run_delay, filter_delay, priority, last_run=0):
    if int(last_run) < int(runs):
        for i in range(int(last_run), int(runs)):
            fils = filters.split('/')
            exps = expdurs.split('/')
            frms = frmcnts.split('/')
            for j in range(len(fils)):
                fil = fils[j]
                exp = exps[j]
                frm = frms[j]
                pre_time = send_obj(obj_name, ra, dec, fil, exp, frm, priority)
                while True:
                    check_obj_status_res = check_obj_status(obj_name, fil, frm, pre_time)
                    if check_obj_status_res:
                        break
                time.sleep(float(filter_delay))
            time.sleep(float(run_delay))

def get_obj_infs(obj):
    #host="10.0.10.236" #"172.28.8.28" 
    try:
        db = psycopg2.connect(host="10.0.10.236",port="5432",database="gwacyw",user="yunwei",password="gwac1234")
    except:#psycopg2.Error :
        return
    else:
        try:
            cur = db.cursor()
            sql = "SELECT * FROM object_list_all WHERE obj_id='%s'" % str(obj)
            cur.execute(sql)
            cols = [it[0] for it in cur.description]
            #print cols
            alls = cur.fetchall()
            #print alls
            alls_dic = {}
            for i in range(len(cols)):
                alls_dic[cols[i]] = str(alls[0][i])
            cur.close()
            db.close()
        except Exception as e:
            print e
            return
        else:
            return alls_dic

def complete_updt(obj):
    try:
        db = psycopg2.connect(host="10.0.10.236",port="5432",database="gwacyw",user="yunwei",password="gwac1234")
    except:#psycopg2.Error :
        return
    else:
        try:
            cur = db.cursor()
            time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = "update pd_log_current set obs_stag='complete', obj_comp_time='%s'  where obj_id='%s' and obs_stag='sent'" % str(time_now, obj)
            cur.execute(sql)
            cur.close()
            db.close()
        except Exception as e:
            print e
            return

if __name__ == "__main__":
    obj = sys.argv[1]
    obj_infs = get_obj_infs(obj)
    obj_name, ra, dec, filters, expdurs, frmcnts, runs, run_delay, filter_delay, priority = at(obj_infs, 'obj_name', 'objra', 'objdec', 'filter', 'expdur', 'frmcnt', 'run_name', 'delay', 'note', 'priority' )
    check_status(obj_name, ra, dec, filters, expdurs, frmcnts, runs, run_delay, filter_delay, priority, last_run=0)
    complete_updt(obj)

