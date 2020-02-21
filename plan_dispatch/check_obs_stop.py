#coding=utf-8

from __future__ import unicode_literals
import subprocess

cmd_in = "ps -ef | grep 'check_obs_start' | grep -v grep"
cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
res = cmd.stdout.read()
if res:
    for it in res[1]:
        pid = it.split()[1]
        #print pid
        cmd_in = 'kill -9 %s' % pid
        cmd = subprocess.Popen(cmd_in, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)      

