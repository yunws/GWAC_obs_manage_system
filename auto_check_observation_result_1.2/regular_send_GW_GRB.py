#-*- coding: utf-8 -*-
#!/usr/bin/env python 
# -*- coding: utf-8 -*-
import time
import os

while True:
    print('game start')
    while True:
        time_now = time.strftime("%H:%M:%S", time.localtime())  # 刷新
#        if time_now == "16:00:00": 
        if time_now == "08:30:00":
            os.system('python3 send_GWAC_observation_log.py')
            time.sleep(10)
            os.system('python3 send_mail.py')
            break
    time.sleep(86340)