#!/usr/bin/python
# -*- coding: utf-8 -*-
#author: Xiao Yujie
#update: 2019-01-13
#version: 0.0.2

import sys, os
from class_object_generator import object_generator as og
from communication_client import Client
import json

def main():
    def creat_kwargs():
        key, value = i.split('=')
        if 'pg' in key:
            print i
            pg_kwargs[key.lower()] = value
        elif 'skip_confirm' in key:
            skip_confirm = value
            return skip_confirm
        else:
            kwargs[key.lower()] = value

    params_list = sys.argv[1:]

    skip_confirm = False
    arg = ''
    args = []
    kwargs = {}
    pg_kwargs = {}

    if not params_list:
        obj_admin = og()
        obj_admin.object_generate()
    
    elif os.path.isfile(params_list[0]) or os.path.isdir(params_list[0]):
        if len(params_list) == 1:
            obj_admin = og()
            obj_admin.object_generate(params_list[0])

        else:
            print params_list
            for i in params_list[1:]:
                if '=' in i:
                    if creat_kwargs():
                        skip_confirm = True
                else:
                    args.append(i)

            obj_admin = og(*args, skip_confirm=skip_confirm, **kwargs)

            if 'observer' in kwargs:
                del kwargs['observer']
            if pg_kwargs:
                obj_admin.set_pg(**pg_kwargs)
            obj_admin.object_generate(params_list[0], **kwargs)

    else:
        for i in params_list:
            if '=' in i:
                if creat_kwargs():
                    skip_confirm = True
            else:
                arg += i + '\t'

        obj_admin = og(skip_confirm=skip_confirm, **kwargs)
        if 'observer' in kwargs:
            del kwargs['observer']
        if pg_kwargs:
            obj_admin.set_pg(**pg_kwargs)

        obj_admin.object_generate(arg, **kwargs)

    if obj_admin.object_params:
        # print skip_confirm
        print u'目标参数：\n'
        for i in obj_admin.object_params:
            print i
        print 'len: ', len(str(obj_admin.object_params))
        pg_action = pg_kwargs['pg_action'] if 'pg_action' in pg_kwargs else 'insert'

        if pg_action == 'insert':
            print u'\n确认写入数据库： Y/n'
        elif pg_action == 'delete':
            print u'\n确认从数据库删除： Y/n'

        if raw_input('>>> ').lower() == 'y':
            obj_admin.pg_proc_object_params(pg_action)
            client = Client('object_generator')
            client.Send()
            raw_input('ok')

def obj_insert(*args, **kwargs):
    # 目标生成器初始化
    obj_admin = og(skip_confirm=True, **kwargs)

    # 生成目标
    obj_admin.object_generate(*args, **kwargs)

    # print obj_admin.object_params # 打印生成的目标

    # 将记录插入object_list_all
    obj_admin.pg_proc_object_params('insert')

    # client = Client('object_generator')
    # client.Send()

if __name__ == "__main__":
    main()