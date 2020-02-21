# coding=utf-8

from __future__ import unicode_literals
import os
import sys
import json
import psycopg2
import time
import datetime
import codecs
import numpy as np 


def load_params(json_file):
    with open(json_file) as read_file:
        pd_params = json.load(read_file)
    return pd_params


def con_db(db_name):
    pd_params = load_params('./pd_params.json')
    try:
        db = psycopg2.connect(**pd_params[db_name])
        return db
    except psycopg2.Error as e:
        print e
        return False


def sql_act(db_name, sql, n=1):
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
            print "\nWARNING: Wrong with operating the db, %s " % str(
                e).strip()
            return False
        finally:
            cur.close()
            db.close()
    else:
        print "\nWARNING: Connection to the db is Error."


if __name__ == "__main__":
    cur_date = datetime.datetime.utcnow().strftime("%Y/%m/%d")
    if sys.argv[1:]:
        date_in = sys.argv[1]
    else:
        date_in = cur_date
    if date_in == cur_date:
        list_in = 'object_list_current'
    else:
        list_in = 'object_list_history'
    while True:
        cur_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")
        if not os.path.exists('obslogs'):
            os.system('mkdir obslogs')
        lf = codecs.open("obslogs/check_followup_%s_%s.txt" %
                         (date_in.replace('/','-'), cur_time), "w", 'utf-8')
        lt1 = []
        lt2 = []
        lt3 = []
        lt_all = []
        lt_t = []
        bad_mak1 = 0
        bad_mak2 = 0
        sql = "select a.obj_id, a.run_name, a.priority, a.expdur, a.frmcnt, a.insert_time, b.tw_begin, b.tw_end, b.obs_stag, b.mode from object_list_all as a, %s  as b where a.obj_id=b.obj_id and a.date_beg='%s' and a.objsour='GWAC_followup' and a.mode='observation' order by a.id" % (
            list_in, date_in)
        res = sql_act('yunwei_db', sql)
        if res:
            all_rec = len(res)
            print 'ALL records:', all_rec, '\n'
            lf.write('#ALL records: %s\n' % str(all_rec))
            lf.write(
                '\n#followup_name, tw_beg, tw_end, obs_stag, objra, objdec, trigger_time, insert_time, time_check, priority, expdur, frmcnt, op_time, dely_time\n')
            for i in res:
                obj_id, fo_name, priority, expdur, frmcnt, insert_time, tw_beg, tw_end, obstag, mode = i[:]
                lt1.append(fo_name)
                sql = "select ra, dec, trigger_time from follow_up_observation where fo_name='"+fo_name+"'"
                res = sql_act('gwac_db', sql)
                if res:
                    objra, objdec, trigger_t = res[0][:]
                    #trigger_t_utc = (trigger_t + datetime.timedelta(hours=-8)).strftime("%Y/%m/%d %H:%M:%S")
                    trigger_t_utc = datetime.datetime.utcfromtimestamp(time.mktime(trigger_t.timetuple())).strftime("%Y/%m/%d %H:%M:%S")
                    trigger_t_utc_mark = time.mktime(time.strptime(trigger_t_utc, "%Y/%m/%d %H:%M:%S"))
                    trigger_t = trigger_t.strftime("%Y/%m/%d %H:%M:%S.%f")
                    if trigger_t_utc >= tw_end:
                        strmark = 'PASS'
                        bad_mak1 += 1
                    elif trigger_t_utc <= tw_beg:
                        strmark = 'WAIT'
                        bad_mak2 += 1
                    else:
                        strmark = 'CURRENT'
                    lt3.append((fo_name, strmark))
                    sql = "select op_time from object_running_list_current where obj_id='"+obj_id+"'"
                    res = sql_act('yunwei_db', sql)
                    if res:
                        op_time = res[0][0]
                    else:
                        sql = "select op_time from object_running_list_history where obj_id='"+obj_id+"'"
                        res = sql_act('yunwei_db', sql)
                        if res:
                            op_time = res[0][0]
                        else:
                            op_time = None
                    insert_time_mark = time.mktime(time.strptime(insert_time, "%Y/%m/%dT%H:%M:%S"))
                    insert_dely_time = str(insert_time_mark - trigger_t_utc_mark) + ' s'
                    if op_time:
                        op_time_mark = time.mktime(time.strptime(op_time, "%Y-%m-%dT%H:%M:%S"))
                        dely_time = (op_time_mark - trigger_t_utc_mark)/60.0
                        dely_time = round(dely_time, 2)
                        lt_all.append(dely_time)
                        if dely_time > 60.0:
                            dely_time = round(dely_time/60.0, 2)
                            dely_time = str(dely_time) + ' hours'
                        else:
                            dely_time = str(dely_time)
                            lt_t.append(dely_time)
                            dely_time = dely_time + ' mins'
                    else:
                        op_time = 'None'
                        dely_time = 'None'
                    print '%s, \033[0;36m%s\033[0m, \033[0;36m%s\033[0m, \033[0;32m%s\033[0m, %s, %s, \033[0;33m%s\033[0m, \033[0;34m%s\033[0m, %s, %s, %s, %s, \033[0;35m%s\033[0m, \033[0;42m%s\033[0m, %s' % (
                        fo_name, tw_beg, tw_end, obstag, objra, objdec, trigger_t, insert_time, strmark, priority, expdur, frmcnt, op_time, dely_time, insert_dely_time)
                    lf.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n' % (
                        fo_name, tw_beg, tw_end, obstag, objra, objdec, trigger_t, strmark, priority, expdur, frmcnt, op_time, dely_time))
                else:
                    print 'No recoder: %s \n' % fo_name
            # print '\n'+' , '.join(lt_t)
            # lf.write('\n'+','.join(lt_t)+'\n')
            lt_tm = []
            for i in lt_t:
                i = float(i)
                if i < 5.0:
                    lt_tm.append(i)
            if lt_tm:
                lt_tm_mean = round(np.mean(lt_tm), 2)
                #lt_tm_median = round(np.median(lt_tm), 2)
                lt_tm_min = round(np.min(lt_tm), 2)
                print '\n'+str(lt_tm_mean)+' , '+str(lt_tm_min)+' , '+str(len(lt_tm))+'/'+str(len(lt_t))+'/'+str(len(lt_all))
                lf.write('\n'+str(lt_tm_mean)+','+str(lt_tm_min)+','+str(len(lt_tm))+'/'+str(len(lt_t))+'/'+str(len(lt_all))+'\n')
            print "\nPASS_number:", bad_mak1, '', "WAIT_number:", bad_mak2, '', "TOTAL:", bad_mak1+bad_mak2
            lf.write("\n#PASS_number: %s , WAIT_number: %s , TOTAL: %s " %
                    (bad_mak1, bad_mak2, bad_mak1+bad_mak2))
            #print lt3
            lt4 = []
            run_n = {}
            for i in lt3:
                fo_name, strmark = i[:]
                obj_name = fo_name[:14]
                if obj_name not in lt4:
                    lt4.append(obj_name)
                    run_n[obj_name] = [1, strmark, []]
                else:
                    k = run_n[obj_name][0]
                    k += 1
                    run_n[obj_name][0] = k
                    if not run_n[obj_name][2] and strmark != run_n[obj_name][1]:
                        run_n[obj_name][2].append((fo_name, strmark))
                    if run_n[obj_name][2] and strmark != run_n[obj_name][2][-1][1]:
                        run_n[obj_name][2].append((fo_name, strmark))
            print "Objects_number:", len(lt4), '\n'
            lf.write("\n#Objects_number: %s \n\n" % str(len(lt4)))
            # #print lt4
            # #print run_n
            # for i in lt4:
            #     if run_n[i][2]:
            #         print i, run_n[i][0], run_n[i][1], run_n[obj_name][2]
            #         str_more = ''
            #         for k in run_n[obj_name][2]:
            #             str_k = ','.join(k)
            #             str_more += '( %s )' % str_k
            #         lf.write('%s %s %s %s\n' %
            #                 (i, str(run_n[i][0]), run_n[i][1], str_more))
            #     else:
            #         print i, run_n[i][0], run_n[i][1]
            #         lf.write('%s %s %s\n' % (i, str(run_n[i][0]), run_n[i][1]))
            # print '\n'
            # lf.write('\n\n')
        fo_date_b = date_in.replace('/', '-') + ' 12:00:00.000'
        #print fo_date_b
        fo_date_e = (datetime.datetime.strptime(date_in, '%Y/%m/%d') +
                     datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 12:00:00.000'
        #print fo_date_e
        sql = "select fo_name from follow_up_observation where trigger_time>'%s' and trigger_time<'%s'" % (
            fo_date_b, fo_date_e)
        res = sql_act('gwac_db', sql)
        if res:
            for i in res:
                lt2.append(i[0])
            #print lt2
        for i in lt2:
            i = i.strip()
            if i not in lt1:
                print i, 'F'
                lf.write('%s F\n' % i)
        lf.close()
        print '\n###########################\n\n'
        time.sleep(10)
        os.system('rm obslogs/check_followup_%s*.txt' %
                  date_in.replace('/', '-'))
