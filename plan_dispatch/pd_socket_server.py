#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import time
import json
import socket
import subprocess
import threading
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def load_params(json_file):
    with open(json_file,'r') as read_file:
        params = json.load(read_file)
    return params

def get_host_ip():
    """
    查询本机ip地址
    :return: ip
    """
    ip = 0
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def get_ser_conf(conf_file):
    socket_confs = load_params(conf_file)['socket']
    ips = {i[0]:i[1] for i in socket_confs.values()}
    ip = get_host_ip()
    if not ip:
        return 0
    if ip in ips:
        port = ips[ip]
        return (ip,int(port))
    else:
        return 0

def pd_socket_server():
    global s, c_pool, beg_mak, end_mak
    s = None
    c_pool = []
    conf_file = './pd_socket_server_params.json'
    beg_mak,end_mak = load_params(conf_file)['socket_mark'][:]
    socket_conf = get_ser_conf(conf_file)
    #socket_conf = (get_host_ip(),33369)
    if socket_conf:
        print 'THIS: ', socket_conf
        s = socket.socket()
        s.bind(socket_conf)
        s.listen(10)
        print 'Server is Ready ...'
        while True:
            c, addr = s.accept()
            c_pool.append(c)
            print "Online：", len(c_pool)
            t_client = threading.Thread(target=handle_msg,args=(c,addr))
            t_client.setDaemon(True)
            t_client.start()
            time.sleep(0.5)

def handle_msg(c,addr):
    global c_pool
    print 'Server:', c, ',','Address: ', addr
    while True:
        try:
            msg = c.recv(1024)
            if not msg:
                break
            elif msg.decode('utf-8') == 'Hi':
                c.send('Hi')
            else:
                print msg, 'From:', addr
                cmd = subprocess.Popen(msg.decode('utf-8','ignore'), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                cmdout = cmd.stdout.read()
                cmderr = cmd.stderr.read()
                lens = len(cmdout.encode('utf-8')) + len(cmderr.encode('utf-8')) + len(end_mak.encode('utf-8'))
                length = format(lens,'%dd'%beg_mak)
                #print length,len(length)
                c.sendall(length + cmdout.encode('utf-8') + cmderr.encode('utf-8') + end_mak.encode('utf-8'))
                print 'End session With:', addr, ';','Send all:', lens
        except Exception as e:
            print 'Wrong in', addr, ':', e
            break
    c.close()
    c_pool.remove(c)
    print "Online：", len(c_pool)


def pd_socket_server_more():
    t_more = threading.Thread(target=pd_socket_server)
    t_more.setDaemon(True)
    t_more.start()
    while True:
        k_in = raw_input()
        if k_in == 'Q':
            for c in c_pool:
                c.close()
            if s:
                s.close()
            break

if __name__ == "__main__":
    pd_socket_server()
    #pd_socket_server_more()