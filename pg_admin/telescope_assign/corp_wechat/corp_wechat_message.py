#!/usr/bin/env python 
# -*- coding: utf-8 -*-

# @Time      :  2018/09/14  20:09
# @Author    :  zrs xyj
# @Site      :
# @File      :  Wechat.py
# @Software  :  PyCharm Community Edition

# !/usr/bin/env python 
# conding:utf-8
# file wechat.py

import time
import requests as requeste
import json 

import sys

reload(sys)
#sys.setdefaultenconding('utf8')


class Wechat:
	def __init__(self):
		self.CORPID = 'wwe1cfc646b5e4823b'
		self.CORPSECRET = '08BIe4r6ojFrQB637wMKg0mQ81c8QagvwUFMvEbGgls'
		self.AGENTID = '1000002'
		self.TOUSER = "@all"  # 接收者用户名
	def _get_access_token(self):
		url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken?'
		values = {'corpid': self.CORPID,
		'corpsecret': self.CORPSECRET,
		}
		print values
		req = requeste.post(url, params=values)
		print req
		data = json.loads(req.text)
		#print data
		return data["access_token"]
	def get_access_token(self):
		try:
			with open('./access_token.conf','r') as f:
				t, access_token = f.read().split()
		except:
			with open('./access_token.conf','w') as f:
				access_token = self._get_access_token()
				cur_time = time.time()
				f.write('\t'.join([str(cur_time),access_token]))
				return access_token
		else:
			cur_time = time.time()
			if 0 < cur_time - float(t) < 7260:
				return access_token
			else:
				with open('./access_token.conf','w') as f:
					access_token = self._get_access_token()
					f.write('\t'.join([str(cur_time),access_token]))
					return access_token
	def send_data(self, message):
		mag = message.encode('utf-8')
		send_url = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=' + self.get_access_token()
		send_data = '{"msgtype": "text", "safe": "0", "agentid": "%s", "touser": "%s", "text": {"content": "%s"}}' % (
			self.AGENTID, self.TOUSER, message)
		r = requeste.post(send_url, send_data)
		print r.content
		return r.content


if __name__ == '__main__':
	wx = Wechat ()
	msgs = raw_input('write msg: ')
	#msgs = sys.argv[1:][0]
	wx.send_data(msgs)
		