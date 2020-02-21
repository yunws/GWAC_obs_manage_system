#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json
import time
import operator
import psycopg2
import paramiko

###
warn_bs = {}
###

def load_params(json_file):
    with open(json_file,'r') as read_file:
        pd_params = json.load(read_file)
    return pd_params

def con_db():
    pd_params = load_params('./pd_params.json')
    try:
        db = psycopg2.connect(**pd_params['yunwei_db'])
    except psycopg2.Error :#as e:
        #print(e)
        return False
    else:
        return db

def sql_act(sql,n=1):
    global warn_bs
    db = con_db()
    if db:
        cur = db.cursor()
        try:
            cur.execute(sql)
            if n == 0:
                db.commit()
                cur.close()
                db.close()
                return
            else:
                rows = cur.fetchall()
                cur.close()
                db.close()
                return rows
        except psycopg2.Error as e:
            #print "\n\033[0;31mWARNING:\033[0m Wrong with operating the db, %s " % str(e).strip()
            #warn_bs['db'] = "WARNING: Wrong with operating the db, " + str(e).strip()
            return False
    else:
        print "\n\033[0;31mWARNING:\033[0m Connection to the db is Error."
        #warn_bs['db'] = "WARNING: Connection to the db is Error."
        return 0

def pg_act(table,action,args=[]):
    if args:
        if action == 'delete':
            cond_keys = args[0].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[0][key]]))
            cond = "' AND ".join(conds)
            sql = "DELETE FROM " + table + " WHERE " + cond + "'"
            sql_act(sql, 0)
        if action == "insert":
            #args = [{'obj_id':'1',}]
            rows = args[0].keys()
            vals = []
            for row in rows:
                vals.append(args[0][row])
            sql = "INSERT INTO " + table + " (" + ", ".join(rows) + ") VALUES ('" + "', '".join(vals) +"')"
            sql_act(sql, 0)
        if action == "update":
            #args = [{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'},{'obj_id':'1','obs_stag':'sent'}]
            rows = args[0].keys()
            targs = []
            for row in rows:
                targs.append("='".join([row,args[0][row]]))
            targ = "' , ".join(targs)
            cond_keys = args[1].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[1][key]]))
            cond = "' AND ".join(conds)
            sql = "UPDATE " + table + " SET " + targ + "' WHERE " + cond + "'"
            sql_act(sql, 0)
        if action == "select":
            #args = [['1','2'],{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'}]
            rows = ','.join(args[0])
            if args[1]:
                cond_keys = args[1].keys()
                conds = []
                for key in cond_keys:
                    conds.append("='".join([key,args[1][key]]))
                cond = "' AND ".join(conds)
                cond += "'"
            else:
                cond = ''
            if len(args) > 2:
                cond_more = args[2]
            else:
                cond_more = ''
            sql = "SELECT " + rows + " FROM " + table + " WHERE " + cond + cond_more
            #print sql
            res = sql_act(sql)
            return res

def con_ssh(ip, username, passwd, cmd, mode=1):
    global warn_bs
    if mode == 1:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        try:
            ssh.connect(hostname=ip, port=22, username=username, password=passwd, timeout=60)
        except Exception as e:
            print "\n\033[0;31mWARNING:\033[0m Connection of ssh is wrong, %s " % e
            #warn_bs['ssh'] = 'WARNING: Connection to %s by ssh is wrong!' % ip
            return 0
        else:
            stdin, stdout, stderr = ssh.exec_command(cmd,get_pty=True)
            out = stdout.readlines()
            # if out:
            #     print '1100'
            #     print out[0]
            err = stderr.readlines()
            # if err:
            #     print '0011'
            #     print err[0]
            ssh.close()
            if not out and not err:
                return 0
            if out and not err:
                return [1,out]
            if err and not out:
                return [2,err]
            if out and err:
                return [3,out,err]
    else:
        try:
            t = paramiko.Transport((ip, 22))
            t.connect(username=username, password=passwd)
            sftp = paramiko.SFTPClient.from_transport(t)
            #cmd = 'put/get t.t pd-tools/'
            cmd_strs = cmd.split(' ')
            cmd_cmd = cmd_strs[0]
            cmd_pth1 = cmd_strs[1]
            cmd_pth2 = cmd_strs[2]
            if cmd_cmd == 'put':
                sftp.put(cmd_pth1, cmd_pth2)
            if cmd_cmd == 'get':
                sftp.get(cmd_pth1, cmd_pth2)
            t.close()
            return 1
        except Exception as e:
            print '\n\033[0;31mWARNING:\033[0m The sftp is wrong, %s ' % e
            return 0

def get_ips_from_confs():
    ssh_socket_confs = load_params('./pd_params.json')['ssh_socket']
    ip_confs = [(ssh_socket_confs[i][0],i) for i in ssh_socket_confs.keys()]
    ip_confs.sort(key=operator.itemgetter(0))
    ip_confs_sort_dic = dict(ip_confs)
    return ip_confs_sort_dic

def check_ser_socket(ip,mode=1,init=1):
    retr = 0
    ip_confs_dic = get_ips_from_confs()
    ssh_socket_confs = load_params('./pd_params.json')['ssh_socket']
    if ip in ip_confs_dic.keys():
        key = ip_confs_dic[ip]
        #print key
        script = 'pd_socket_server.py'
        cmd = 'ps -ef | grep %s | grep -v grep' % script
        #print cmd
        sip, sun, spw = ssh_socket_confs[key][:3]
        #print sip, sun, spw
        res = con_ssh(sip, sun, spw, cmd)
        if res and res[0] == 1:
            retr = 1
        else:
            mode = 0
    else:
        return 0
    if mode == 0:
        print '\nRestart the socket server of %s.' % sip 
        if retr == 1:
            for item in res[1]:
                pid = item.split()[1]
                #print pid
                cmd = 'kill %s' % pid
                print cmd
                res = con_ssh(sip, sun, spw, cmd)
                #print res
                time.sleep(10)
        if init == 0:
            port = ssh_socket_confs[key][3]
            cmd = 'iptables -I INPUT -p tcp --dport %s -j ACCEPT && service iptables save' % port
            root_pw = ssh_socket_confs[key][4]
            con_ssh(sip, 'root', root_pw, cmd)
        socket_ser_path = '/home/' + ssh_socket_confs[key][1] + '/pd-socket'
        #print socket_ser_path
        cmd = 'mkdir %s' % socket_ser_path
        #print cmd
        res = con_ssh(sip, sun, spw, cmd)
        #print res
        f1 = 'pd_socket_server_params.json'
        f2 = 'boot_server.sh'
        cmd = 'put %s %s/%s' % (script, socket_ser_path, script)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd, mode=2)
        #print res
        cmd = 'put %s %s/%s' % (f1, socket_ser_path, f1)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd, mode=2)
        #print res
        cmd = 'put %s %s/%s' % (f2, socket_ser_path, f2)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd, mode=2)
        #print res
        cmd = 'cd %s/ && nohup bash %s %s > /dev/null 2>&1 ' % (socket_ser_path, f2, script)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd)
        #print res
        cmd = 'ps -ef | grep %s | grep -v grep' % script
        sip, sun, spw = ssh_socket_confs[key][:3]
        res = con_ssh(sip, sun, spw, cmd)
        #print res
        if res and res[0] == 1:
            return 1
        else:
            print '\n\033[0;31mWARNING:\033[0mThe socket server of %s is not on-line' % ip
            return 0
    else:
        return retr

def check_ser_socket_background(init=1):
    while True:
        ip_confs_dic = get_ips_from_confs()
        for ip in ip_confs_dic.keys():
            #print ip
            if init == 0:
                check_ser_socket(ip,0,0)
            elif init == -1:
                check_ser_socket(ip,0)
            else:
                check_ser_socket(ip)
        if init in [0,-1]:
            return
        time.sleep(30)

def check_F30_sync(mode=1):
    global warn_bs
    cam_ip, cam_un, cam_pw = load_params('./pd_params.json')['ssh_socket']['30_cam'][:3]
    scr1 = 'sync_re_sh.sh'
    scr2 = 'sync_remote_by_inotify.sh'
    scr3 = 'boot_rsync.sh'
    if mode == 0:
        bk = 1
    else:
        bk = 0
    while True:
        retr = 0
        cmd = 'ps -ef | grep %s | grep -v grep' % scr2
        res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
        if res and res[0] == 1:
            retr = 1
        else:
            mode = 0
        if mode == 0:
            print '\n\033[0;31m########## Restart the sync_remote_by_inotify.sh of F30 in /home/ccduser/sync !\033[0m\n'
            #warn_bs['ssh'] = '\nPlease restart the sync_remote_by_inotify.sh of F30 in /home/ccduser/sync !'
            if retr == 1:
                for item in res[1]:
                    pid = item.split()[1]
                    cmd = 'kill %s' % pid
                    print cmd, scr2
                    con_ssh(cam_ip, cam_un, cam_pw, cmd)
            cmd = 'ps -ef | grep "inotifywait.*/home/ccduser/data/" | grep -v grep'
            res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
            if res:
                for item in res[1]:
                    pid = item.split()[1]
                    cmd = 'kill %s' % pid
                    print cmd, 'inotifywait'
                    con_ssh(cam_ip, cam_un, cam_pw, cmd)
            path = '/home/ccduser/sync'
            cmd = 'mkdir %s' % path
            con_ssh(cam_ip, cam_un, cam_pw, cmd)
            cmd = 'put %s %s/%s' % (scr1, path, scr1)
            con_ssh(cam_ip, cam_un, cam_pw, cmd, mode=2)
            cmd = 'put %s %s/%s' % (scr2, path, scr2)
            con_ssh(cam_ip, cam_un, cam_pw, cmd, mode=2)
            cmd = 'put %s %s/%s' % (scr3, path, scr3)
            con_ssh(cam_ip, cam_un, cam_pw, cmd, mode=2)
            cmd = 'cd %s/ && nohup bash %s %s > /dev/null 2>&1' % (path, scr3, scr2)
            #print cmd
            con_ssh(cam_ip, cam_un, cam_pw, cmd)
            cmd = 'ps -ef | grep %s | grep -v grep' % scr2
            res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
            if not res:
                print '\n\n\033[0;31m########## Please restart it by hand !\033[0m\n\n'
            if bk == 1:
                break
        time.sleep(300)

def get_ser_config(group_id,cam_id='0'):
    if group_id == 'XL001':
        conf_key = 'GWAC_ser'
    if group_id == 'XL002':
        if cam_id == '1':
            conf_key = 'w60_ser'
        if cam_id == '2':
            conf_key = 'e60_ser'
    if group_id == 'XL003':
        conf_key = 'F30_ser'
    ser_ip = load_params('./pd_params.json')['ssh_socket'][conf_key][0]
    ser_port = load_params('./pd_params.json')['ssh_socket'][conf_key][3]
    ser_conf_list = [ser_ip, ser_port]
    return ser_conf_list

def get_cam_config(cam_id):
    if cam_id == '1':
        conf_key = 'w60_cam'
    if cam_id == '2':
        conf_key = 'e60_cam'
    if cam_id == '3':
        conf_key = '30_cam'
    cam_ip = load_params('./pd_params.json')['ssh_socket'][conf_key][0]
    cam_port = load_params('./pd_params.json')['ssh_socket'][conf_key][3]
    cam_conf_list = [cam_ip, cam_port]
    return cam_conf_list

if __name__ == "__main__":
    #check_ser_socket()
    #print get_ips_from_confs()
    # ip = '172.28.1.11'
    # xx = check_ser_socket(ip,mode=0)
    # print xx
    #check_ser_socket_background(init=0)
    #check_ser_socket_background(-1)
    #check_F30_sync(mode=1)
    check_ser_socket('190.168.1.203',mode=0,init=0)
