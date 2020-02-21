#!/usr/bin/env python
#coding=utf-8

from __future__ import unicode_literals
import os, sys, json, psycopg2, time, datetime, codecs


def load_params(json_file = './pd_params.json'):
    with open(json_file,'r') as read_file:
        pd_params = json.load(read_file)
    return pd_params

def con_db(db_name):
    pd_params = load_params()
    try:
        db = psycopg2.connect(**pd_params[db_name])
    except psycopg2.Error as e:
        print(e)
        return False
    else:
        return db

def sql_act(db_name,sql,n=1):
    db = con_db(db_name)
    if db:
        cur = db.cursor()
        try:
            if n == 0:
                cur.execute(sql)
                db.commit()
            else:
                cur.execute(sql)
                rows = cur.fetchall()
                return rows
        except psycopg2.Error as e:
            print "\nWARNING: Wrong with operating the db, %s " % str(e).strip()
            return False
        finally:
            cur.close()
            db.close()
    else:
        print "\nWARNING: Connection to the db is Error."

def init(mark=2):
    global lf_fu
    cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    if not os.path.exists('obslogs'):
        os.system('mkdir obslogs')
    #lf_fu = open("obslogs/follow_up_log_%s.txt" % cur_date, "a+")
    lf_fu = codecs.open("obslogs/follow_up_log_%s.txt" % cur_date, "a+", 'utf-8')
    if mark == 1:
        time_mark = cur_date + ' 08:00:00.000'
        sql = "SELECT fo_id FROM follow_up_observation WHERE trigger_time > '%s' ORDER BY fo_id LIMIT 1" % time_mark
        ids = sql_act('gwac_db',sql)
        if ids:
             return int(ids[0][0])-1
        else:
            #print 'NO recod.'
            return 0
    if mark == 2:
        date_now = datetime.datetime.utcnow().strftime("%Y/%m/%d")
        sql = "SELECT note FROM object_list_all WHERE objsour='GWAC_followup' and date_beg='%s' ORDER BY id DESC LIMIT 1" % date_now
        res = sql_act('yunwei_db',sql)
        if res:
            note_id = res[0][0]
            if note_id:
                return int(note_id)
            else:
                return 0
        else:
            return 0
    if mark == 3:
        time_now = (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))[:-3]
        sql = "SELECT fo_id FROM follow_up_observation WHERE trigger_time > '%s' ORDER BY fo_id LIMIT 1" % time_now
        ids = sql_act('gwac_db',sql)
        if ids:
             return int(ids[0][0])-1
        else:
            #print 'NO recod.'
            return 0

def pd_followup():
    from communication_client import Client
    from object_generator import obj_insert
    global lf_fu
    ###
    obj_generator = Client('object_generator')
    ###
    mark_id = 0
    init_res = init()
    if init_res:
        mark_id = init_res
    i = 0
    lf_fu.write('\n\n\nRestart at: %s' % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    while True:
        cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        time_h = datetime.datetime.now().strftime("%H:%M")#time.strftime("%H:%M", time.localtime(time.time()))
        if time_h == '8:05':
            lf_fu.close()
            lf_fu = codecs.open("obslogs/follow_up_log_%s.txt" % cur_date, "a+", 'utf-8')
            lf_fu.write("# obj_name objrank begin_time end_time group_id\n")
            time.sleep(60)
        i += 1
        if not mark_id:
            sql = "SELECT fo_id FROM follow_up_observation ORDER BY fo_id DESC LIMIT 1"
            res = sql_act('gwac_db',sql)
            if res:
                mark_id = res[0][0]
            # else:
            #     print 'Wrong WITH GETTING THE fo_id.'
        else:
            sql = "SELECT fo_id FROM follow_up_observation WHERE fo_id > " + str(mark_id) + " ORDER BY fo_id"
            ids = sql_act('gwac_db',sql)
            if ids:
                lf_fu.write('\n\nRun times：%s \n' % str(i))
                lf_fu.write('Last id : %s \n' % str(mark_id))
                fo_id = 0
                for id in ids:
                    fo_id = id[0]
                    sql = "SELECT fo_name, user_id, ra, dec, epoch, expose_duration, frame_count, filter, priority, trigger_time, telescope_id, obj_name FROM follow_up_observation WHERE fo_id = " + str(fo_id)
                    res = sql_act('gwac_db',sql)
                    if res:
                        run_name, user_id, objra, objdec, epoch, expdur, frmcnt, filter, priority, trigger_time, telescope_id, obj_name = res[0][:]
                        try:
                            date_indb = (trigger_time + datetime.timedelta(hours= -8)).strftime("%Y/%m/%d")
                        except:
                            date_indb = datetime.datetime.utcnow().strftime("%Y/%m/%d")
                        if telescope_id == 1:
                            group_id = 'XL002'
                        if telescope_id == 2:
                            group_id = 'XL003'
                        observer = 'GWAC'
                        # if user_id == 0:
                        #     observer = 'GWAC'
                        # else:
                        #     sql = "SELECT login_name FROM user_info WHERE ui_id = " + str(user_id)
                        #     observer = sql_act('gwac_db',sql)[0][0]
                        if int(priority) < 80:
                            priority = str(int(priority)-40+80)
                        obj_line = "obj_name=%s objsour=GWAC_followup observer=%s objra=%s objdec=%s objepoch=%s objerror=0.0|0.0 objrank=0 group_id=%s unit_id=002 obs_type=goa obs_stra=pointing date_beg=%s date_end=%s day_int=0 imgtype=object filter=%s expdur=%s delay=0 frmcnt=%s priority=%s run_name=%s note=%s mode=observation\n" % \
                            (obj_name, observer, objra, objdec, epoch, group_id, date_indb, date_indb, filter, expdur, frmcnt, priority, run_name, str(id[0]))
                        lf_fu.write(obj_line + str(trigger_time) + '\n')
                        obj_insert(obj_line)
                mark_id = int(fo_id)
                obj_generator.Send("Hello World",['insert'])
        time.sleep(0.1)

if __name__ == "__main__":
    mark_id = init()
    mark_id = 30104
    i = 0
    lf_fu.write('\n\n\nRestart at: %s' % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    while True:
        i += 1
        print 'Run times：%s \n' % i
        print mark_id
        lf_fu.write('\n\nRun times：%s \n' % str(i))
        if not mark_id:
            sql = "SELECT fo_id FROM follow_up_observation ORDER BY fo_id DESC LIMIT 1"
            res = sql_act('gwac_db',sql)
            if res:
                mark_id = res[0][0]
        else:
            lf_fu.write('Last id : %s \n' % str(mark_id))
            sql = "SELECT fo_id FROM follow_up_observation WHERE fo_id > " + str(mark_id) + " ORDER BY fo_id"
            ids = sql_act('gwac_db',sql)
            if ids:
                print ids, len(ids)
                fo_id = 0
                for id in ids:
                    fo_id = id[0]
                    sql = "SELECT fo_name, user_id, ra, dec, epoch, expose_duration, frame_count, filter, priority, trigger_time, telescope_id, obj_name FROM follow_up_observation WHERE fo_id = " + str(fo_id)
                    res = sql_act('gwac_db',sql)
                    if res:
                        run_name, user_id, objra, objdec, epoch, expdur, frmcnt, filter, priority, trigger_time, telescope_id, obj_name = res[0][:]
                        try:
                            #date_indb = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(str(trigger_time)[:-3], "%Y-%m-%d %H:%M:%S.%f"))).strftime("%Y/%m/%d")
                            date_indb = (trigger_time + datetime.timedelta(hours= -8)).strftime("%Y/%m/%d")
                        except:
                            date_indb = datetime.datetime.utcnow().strftime("%Y/%m/%d")
                        if telescope_id == 1:
                            group_id = 'XL002'
                        if telescope_id == 2:
                            group_id = 'XL003'
                        if user_id == 0:
                            observer = 'GWAC'
                        else:
                            sql = "SELECT login_name FROM user_info WHERE ui_id = " + str(user_id)
                            observer = sql_act('gwac_db',sql)[0][0]
                        priority = str((int(priority) - 40) + 80)
                        obj_line = "obj_name=%s objsour=GWAC_followup observer=%s objra=%s objdec=%s objepoch=%s objerror=0.0|0.0 objrank=0 group_id=%s unit_id=001 obs_type=goa obs_stra=pointing date_beg=%s date_end=%s day_int=0 imgtype=object filter=%s expdur=%s delay=0 frmcnt=%s priority=%s run_name=%s note=%s mode=observation\n" % \
                            (obj_name, observer, objra, objdec, epoch, group_id, date_indb, date_indb, filter, expdur, frmcnt, priority, run_name, str(id[0]))
                        print obj_line
                        lf_fu.write(obj_line + str(trigger_time) + '\n')
                    time.sleep(1)
                #print fo_id
                mark_id = int(fo_id)
        time.sleep(5)
