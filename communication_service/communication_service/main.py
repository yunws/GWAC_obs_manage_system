#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'Yujie Xiao'
__create_time__ = ''
__update_time__ = ''
__version__ = '0.1'

#创建SocketServerTCP服务器：
import SocketServer
from SocketServer import StreamRequestHandler as SRH
import threading
import json
import time
from configure import *
# recv_json '{"type": $type, "content": {}, 'time': $time[, "pg_action": [$pg_action[, $table_name[, $column_name] ] ] ] }\n'
# send_json '{"error": 0/1[, "type": $type, "content": {}, 'time': $time] }\n'

def socket_error_msg(msg):
    if msg[0] == 10061:
        print 'Cannot Connect Service'
    elif msg[0] == 10054:
        print 'Connection Break'
    elif msg[0] == 9:
        print '\nkeep connection subprocess break'
    elif msg[0] == 104:
        print 'Connection reset by peer'
    else:
        print 'Unkown error'
    print "Creating Socket Failure. Error Code : " + str(msg[0]) + " Message : " + msg[1] + '\n'

class Servers(SRH):
    sort_dict_lock = threading.Lock()
    dispatch_dict_lock = threading.Lock()
    control_dict_lock = threading.Lock()
    object_sort_dict = {}
    plan_dispatch_dict = {}
    pgdb_modify_dict = {}
    client_control_dict = {}
    client_type = ''

    def sendall_len(self, data, sock=''):
        data = '%06d' % len(data) + data
        if sock:
            sock.sendall(data)
        else:
            self.request.sendall(data)

    def heartbeat(self):
        while True:
            try:
                self.sendall_len('10000\n')
            except SocketServer.socket.error, msg:
                print self.request.getpeername(),
                socket_error_msg(msg)
                self.request.close()
                break
            time.sleep(90)

    def del_dict(self, client_type, *del_list, **kwargs):
        mode = kwargs['mode'] if 'mode' in kwargs else 'non-self'
        for i in del_list:
            if client_type == 'object_sort' and mode != 'self' or client_type == 'plan_dispatch' and mode == 'self':
                self.dispatch_dict_lock.acquire()
                if i in self.plan_dispatch_dict:
                    del self.plan_dispatch_dict[i]
                    print 'del ', i, 'from plan_dispatch_list'
                self.dispatch_dict_lock.release()
            elif client_type == 'plan_dispatch' and mode != 'self' or client_type == 'object_sort' and mode == 'self':
                self.sort_dict_lock.acquire()
                if i in self.object_sort_dict:
                    del self.object_sort_dict[i]
                    print 'del ', i, 'from object_sort_list'
                self.sort_dict_lock.release()
            elif client_type == 'client_control':
                self.control_dict_lock.acquire()
                if i in self.client_control_dict:
                    del self.client_control_dict[i]
                    print 'del ', i, 'from client_control_list'
                self.control_dict_lock.release()
            self.control_recv_process(i + ' is offline.')   

    def control_recv_process(self, data):
        _send_dict = self.client_control_dict
        if _send_dict:
            _remove_list = []
            for i in _send_dict:
                try:
                    
                    Online_client = {'object_sort': self.object_sort_dict.keys(), 'plan_dispatch': self.plan_dispatch_dict.keys()}
                    self.sendall_len(json.dumps(Online_client) + '\n', _send_dict[i])
                    self.sendall_len(json.dumps(data) + '\n', _send_dict[i])
                except SocketServer.socket.error, msg:
                    print i, # getpeername()在socket关闭后不可用
                    socket_error_msg(msg)
                    _remove_list.append(i)
            if _remove_list:
                self.del_dict('client_control', *_remove_list)

    def response_process(self):
        while True:
            _remove_list = []
            send_flag = False
            bufsize = self.request.recv(6)
            print bufsize
            bufsize = int(bufsize)
            recv_data = self.request.recv(bufsize)
            data = json.loads(recv_data)
            self.client_type = data['type']
            client_type = data['type']

            if client_type == 'object_sort' or client_type == 'plan_dispatch' or client_type == 'object_generator':
                if client_type == 'object_sort':
                    if str(self.client_address) not in self.object_sort_dict:
                        self.object_sort_dict[str(self.client_address)] = self.request
                        print 'object_sort: ', self.object_sort_dict.keys()
                    _send_dict = self.plan_dispatch_dict
                else:
                    if client_type == 'plan_dispatch':
                        if str(self.client_address) not in self.plan_dispatch_dict:
                            self.plan_dispatch_dict[str(self.client_address)] = self.request
                            print 'plan_dispatch :', self.plan_dispatch_dict.keys()
                            print data
                    _send_dict = self.object_sort_dict

                if 'content' in data:
                    if _send_dict:
                        for i in _send_dict:
                            try:
                                data.update({'error': 0})
                                send_data = json.dumps(data)
                                if client_type == 'object_generator':
                                    print send_data
                                    print _send_dict
                                if 'pg_action' in data:
                                    print send_data
                                self.sendall_len(send_data + '\n', _send_dict[i])
                                send_flag = True
                                break
                            except SocketServer.socket.error, msg:
                                print i
                                socket_error_msg(msg)
                                _remove_list.append(i)

                        if _remove_list:
                            self.del_dict(client_type, *_remove_list)

                    if not send_flag:
                        error_msg = json.dumps({'error':1}) + '\n'
                        self.sendall_len(error_msg)

                else:
                    self.sendall_len('connection %s:%s at %s succeed!' % (host, port, time.ctime()))

                self.control_recv_process(data)

            elif client_type == 'client_control':
                if str(self.client_address) not in self.client_control_dict:
                    self.client_control_dict[str(self.client_address)] = self.request
                    _send_dict = self.plan_dispatch_dict

                if 'content' in data:
                    if _send_dict:
                        for i in _send_dict:
                            try:
                                data.update({'error': 0})
                                send_data = json.dumps(data)
                                if 'pg_action' in data:
                                    print send_data
                                self.sendall_len(send_data + '\n', _send_dict[i])
                                send_flag = True
                                break
                            except SocketServer.socket.error, msg:
                                print i
                                socket_error_msg(msg)
                                _remove_list.append(i)

                        if _remove_list:
                            self.del_dict(client_type, *_remove_list)

                    if not send_flag:
                        error_msg = json.dumps({'error':1}) + '\n'
                        self.sendall_len(error_msg)

                self.control_recv_process(data)

            else:
                self.sendall_len('unsupported client type: ' + recv_data)
                self.request.shutdown(2)
                self.request.close()
                raise KeyError(
                    u'unsupported client type: ', data
                ) 

    def handle(self):
        print 'got connection from ',self.client_address
        
        #a=threading.Thread(target=self.heartbeat)
        #a.start()

        try:
            self.response_process()
        
        except SocketServer.socket.error, msg:
            print self.client_type
            print self.client_address,
            socket_error_msg(msg)
        except TypeError, msg:
            print msg, self.client_address, self.request
        except KeyError, msg:
            print msg
        finally:
            self.del_dict(self.client_type, str(self.client_address), mode='self')
            self.request.shutdown(2)
            self.request.close()

def start_server():
    if localhost:
        addr = ('127.0.0.1',10086)
        print 'start localhost test\n'
    else:
        addr = (host,port)

    print 'server is running....'  
    server = SocketServer.ThreadingTCPServer(addr,Servers)  
    server.serve_forever()

if __name__ == '__main__':
    start_server()
