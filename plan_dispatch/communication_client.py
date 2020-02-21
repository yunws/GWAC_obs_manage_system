#!/usr/bin/python
# -*- coding: utf-8 -*-

__version__ = '0.2'

import socket, json, time
import threading

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

class Client:

    def __init__(self, client_type=''):

        Client.__author__ = "Yujie Xiao"

        # self.conf = 'client.conf'

        self.client_type = client_type or 'object_sort'
        self.host = '127.0.0.1'
        self.port = 10086
        # self.bufsize = 5120
        self.send_data_base = {'type':self.client_type}

        self.conn()

    def conn(self, new=False):
        _client = socket.socket()
        _client.connect((self.host, self.port))
        if new:
            return _client
        self.client = _client
        self.sendall_len(json.dumps(self.send_data_base))
        self.Recv()

    def Recv(self, sock=''):
        if not sock:
            sock = self.client
        bufsize = int(sock.recv(6))
        recv_data = sock.recv(bufsize)
        try:
            _recv_data = json.loads(recv_data)
        except:
            _recv_data = recv_data
        if not sock:
            return _recv_data
        self.recv_data = _recv_data
        #print self.recv_data

    def Send(self, content='', pg_action=[]):
        if self.client_type == 'pgdb_modify':
            data = self.recv_data
            del data['pg_action']
        else:
            data = self.send_data_base
            #if content and self.client_type != 'object_generator':
            if 'pg_action' in content and not pg_action:
                data['pg_action'] = content['pg_action']
                del content['pg_action']
            elif 'pg_action' not in content and pg_action and type(pg_action) == type([]):
                data['pg_action'] = pg_action
            elif 'pg_action' not in content and not pg_action:
                pass
            else:
                print('Wrong $pg_action')
            data['content'] = content
            # elif self.client_type == 'object_generator':
            #     data['pg_action'] = ['insert']
            #     data['content'] = 'Hello World!'
        data['time'] = time.time()
        self.sendall_len(json.dumps(data))

    def sendall_len(self, data, sock=''):
        data = '%06d' % len(data) + data
        if not sock:
            sock = self.client
        sock.sendall(data)

    def online_client(self):
        _client = self.conn(new=True)
        _data = {'type': 'client_control'}
        self.sendall_len(json.dumps(_data), _client)
        _recv_data = self.Recv(_client)
        return _recv_data
        



if __name__ == "__main__":
    client = Client()
    a=threading.Thread(target=client.Recv)
    a.start()
    time.sleep(1)
    print client.online_client()
    time.sleep(1)
    while True:
        data = raw_input('>>> ')
        client.Send(data)
