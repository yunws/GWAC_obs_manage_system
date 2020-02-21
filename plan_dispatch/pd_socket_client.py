#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, json, socket, threading

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

def pd_socket_client_main(host,port,xcmd):
    beg_mak,end_mak = load_params('./pd_params.json')['socket_mark'][:]
    if xcmd:
        _s = None
        try:
            _s = socket.socket()
            _s.connect((host, int(port)))
            _s.send(xcmd.encode('utf-8'))
            len_date = _s.recv(beg_mak)
            #print len_date,len(len_date)
            if int(len_date) == len(end_mak):
                _s.close()
                return ''
            count = 1024
            while True:
                a = int(len_date) % count
                if a:
                    break
                count += 1
            data = ''
            while True:
                data_cont = _s.recv(count)
                data += data_cont
                if (len(data_cont) < count) and (end_mak in data_cont.decode('utf-8')):
                    data = data[:-len(end_mak)]
                    break
            _s.close()
            return ((data.decode('utf-8')).strip()).split('\n')
        except Exception as e:
            print '\nWRONG: %s\n' %e
            if _s:
                _s.close()
            return 0
    else:
        print '\n\nWARNING: [SOCKET] The cmd is null.\n\n'
        return 0

class MyThread_pd_socket_client(threading.Thread):
    def __init__(self, target=None,
                 args=(), kwargs={}):
        super(MyThread_pd_socket_client, self).__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs
        #self.__result = None
    def run(self):
        if self.target == None:
            return
        else:
            self.__result = self.target(*self.args, **self.kwargs)
    def get_result(self):
        self.join(30)
        try:
            return self.__result
        except Exception:
            #print 'Broken Runing.'
            return

def pd_socket_client(host,port,xcmd):
    t = MyThread_pd_socket_client(target=pd_socket_client_main, args=(host,port,xcmd))
    t.setDaemon(True)
    t.start()
    return t.get_result()

class MyThread_check_ser_online(threading.Thread):
    def __init__(self):
        super(MyThread_check_ser_online, self).__init__()
        self.__pause_flag = threading.Event()
        self.__pause_flag.set()
        self.__stop_flag = threading.Event()
        self.__stop_flag.set()
        self.exitcode = 0
        self.exception = None
    def run(self):
        global s, cmd
        while self.__stop_flag.isSet():
            self.__pause_flag.wait()
            if s:
                s.send('Hi'.encode('utf-8'))
                msg = s.recv(1024)
                if not msg:
                    s.close()
                    s = None
                    break
        if self.__stop_flag.isSet():
            self.exitcode = 1
            self.exception = 'Lost server'
    def pause(self):
        self.__pause_flag.clear()
    def pause(self):
        self.__pause_flag.set()
    def stop(self):
        self.__pause_flag.set()
        self.__stop_flag.clear()
        self.join()

def type_input():
    global cmd
    xcmd = raw_input(">>> ").strip()
    if not xcmd:
        xcmd = 'None'
    cmd = xcmd

def pd_socket_client_more(host,port):
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    global cmd, s
    beg_mak,end_mak = load_params('./pd_params.json')['socket_mark'][:]
    s = None
    s = socket.socket()
    s.connect((host, int(port)))
    i = 0
    while True:
        if s:
            try:
                cmd = ''
                ### check_ser_onlie
                t_ck = MyThread_check_ser_online()
                t_ck.setDaemon(True)
                t_ck.start()
                t_in = threading.Thread(target=type_input)
                t_in.setDaemon(True)
                t_in.start()
                while not cmd:
                    if t_ck.exitcode == 1:
                        raise Exception(t_ck.exception)
                ### end check_ser_onlie
                t_ck.stop()
                if cmd == 'None':
                    continue
                if cmd == 'Q':
                    break
                print 'CMD: { %s }' % cmd
                ##############################
                s.send(cmd.encode('utf-8'))
                len_date = s.recv(beg_mak)
                print int(len_date),len(len_date)
                time.sleep(0.5)
                if int(len_date) == len(end_mak):
                    continue
                count = 1024
                while True:
                    a = int(len_date) % count
                    if a:
                        break
                    count += 1
                data = ''
                while True:
                    data_cont = s.recv(count)
                    data += data_cont
                    if (len(data_cont) < count) and (end_mak in data_cont.decode('utf-8')):
                        data = data[:-len(end_mak)]
                        break
                print data.decode('utf-8','ignore')
            except Exception as e:
                print '\n\nWrong(%s): exit\n' % e
                if s:
                    s.close()
                    s = None
                break
        else:
            break

if __name__ == "__main__":
    host = '190.168.1.203'#get_host_ip()
    port = '33369'
    pd_socket_client_more(host,port)
    # cmd = 'ls /tmp/gfts*.log | sort -r | head -1'
    # cmd = 'ls aaa'
    # cmd = '你好'
    # res = pd_socket_client(host,port,cmd)
    # print res,res[0]
    # #res = res.strip().split('\n')
    # print res,len(res)
    # k = 0
    # for i in res:
    #     k += 1
    #     print i,k
    # beg_mak = 10
    # lens = 333
    # length = format(lens,'%dd'%beg_mak)
    # print length,len(length),len(length.encode('utf-8'))
