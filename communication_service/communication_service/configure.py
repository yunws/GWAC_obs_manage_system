#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyetc
import os

configfile = 'server.conf'

conf = pyetc.load(configfile)

host = conf.host
localhost = conf.localhost
port = conf.port
bufsize = conf.bufsize
support_client_type = conf.support_client_type