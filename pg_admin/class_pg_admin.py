#!/usr/bin/python
# -*- coding: utf-8 -*-
#author: Xiao Yujie
#update: 2019-11-25
#version: 0.0.5

import psycopg2
import psycopg2.extras
import time, sys

class pg_admin:
    def __init__(self, *args, **kwargs):
        self.set_pg(init=True, **kwargs)

    def set_pg(self, **kwargs):
        if 'init' in kwargs and kwargs['init']:
            # self.pg_host = '10.0.10.236'
            self.pg_host = '172.28.8.28'
            self.pg_port = '5432'
            self.pg_user = 'yunwei'
            self.pg_passwd = 'gwac1234'
            self.pg_db = 'gwacyw'

        self.pg_host = kwargs['pg_host'] if 'pg_host' in kwargs else self.pg_host
        self.pg_port = kwargs['pg_port'] if 'pg_port' in kwargs else self.pg_port
        self.pg_user = kwargs['pg_user'] if 'pg_user' in kwargs else self.pg_user
        self.pg_passwd = kwargs['pg_passwd'] if 'pg_passwd' in kwargs else self.pg_passwd
        self.pg_db = kwargs['pg_db'] if 'pg_db' in kwargs else self.pg_db

        # self.key_in_pgdb = self.get_keys_in_pgdb()

        return (self.pg_host, self.pg_port, self.pg_user, self.pg_passwd, self.pg_db)

    def pg_conn(self):
        try:
            conn = psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_user, password=self.pg_passwd, database=self.pg_db)
            return conn
        except psycopg2.Error as a:
            print(a)#, u'\n数据库连接异常：', (self.pg_host, self.pg_port, self.pg_user, self.pg_passwd, self.pg_db)
            return False

    def get_keys_in_pgdb(self, pg_table_name='object_list_current'):
        conn = self.pg_conn()
        if conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                sql = "select column_name from information_schema.columns where table_schema='public' and table_name='" + pg_table_name + "' limit 1"
                cur.execute(sql)
                rows = cur.fetchall()
                _key_in_pgdb = []
                for i in rows:
                    _key_in_pgdb.append(i[0])
                return _key_in_pgdb
            except psycopg2.Error as a:
                print(a)
                return False
            finally:
                conn.close()

    def get_date_in_pgdb(self, pg_table_name='object_list_current'):
        conn = self.pg_conn()
        if conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                sql = "select date_cur from " + pg_table_name + " limit 1"
                cur.execute(sql)
                rows = cur.fetchall()
                return rows[0][0] if rows else False
            except psycopg2.Error as a:
                print(a)
                return False
            finally:
                conn.close()

    def current_to_history(self):
        conn = self.pg_conn()
        if conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                sql = 'insert into object_list_history (obj_id, date_cur, tw_begin, tw_end, obs_stag, mode) SELECT obj_id, date_cur, tw_begin, tw_end, obs_stag, mode FROM object_list_current'
                cur.execute(sql)
                conn.commit()
                return True
            except psycopg2.Error as a:
                print(a)
                return False
            finally:
                conn.close()

    def pg_extrs(self, conn, cur, object_params_dict={}, pg_action=[]):
        action = pg_action[0].lower()
        pg_table_name = pg_action[1]
        current_date = time.strftime('%Y/%m/%d', time.gmtime())
        if action in ['daily_init', 'insert_init', 'set_default', 'sort_debug']:

            if action == 'daily_init':
                sql = 'insert into ' + pg_table_name + ' (obj_id, mode) \
                    (SELECT a.obj_id, a.mode FROM object_list_all a , object_list_history c where \
                    ($$' + current_date + '$$ between a.date_beg and a.date_end) and\
                    a.obj_id = c.obj_id and c.obs_stag <> $$complete$$ and\
                    to_date($$' + current_date + '$$,$$yyyy/mm/dd$$) + 1 - to_date(c.date_cur,$$yyyy/mm/dd$$) >= a.day_int)'
                cur.execute(sql)
                sql = 'update object_list_all set obs_stag=$$pass$$ where date_end < $$' + current_date + '$$'
                cur.execute(sql)
                sql = 'update ' + pg_table_name + ' set obs_stag = $$initial$$ where obs_stag is null'

            elif action == 'insert_init':
                sql = 'insert into ' + pg_table_name + ' (obj_id, mode) \
                    (SELECT obj_id, mode FROM object_list_all where \
                    date_beg = $$' + current_date + '$$ and not exists \
                    (select obj_id from ' + pg_table_name + ' b where b.obj_id = object_list_all.obj_id))'
                cur.execute(sql)
                sql = 'update ' + pg_table_name + ' set obs_stag = $$initial$$ where obs_stag is null'

            elif action == 'set_default':
                sql = 'update object_list_all set obj_id = id where obj_id is null'

            elif action == 'sort_debug':
                obj_id = object_params_dict['obj_id']
                sql = 'select case when \
                    (exists (select obj_id from ' + pg_table_name + ' where obj_id = $$%s$$)) \
                    then (select obs_stag from ' + pg_table_name + ' where obj_id = $$%s$$) \
                    else $$%s not exists$$ end' % (obj_id, obj_id, obj_id)
            cur.execute(sql)

            if action == 'sort_debug':
                rows = cur.fetchall()
                if rows:
                    return rows
                else:
                    return False
            else:
                conn.commit()
        else:
            print('pg_action error: aciton =', pg_action)
            sys.exit()

    def pg_process(self, object_params_dict={}, pg_action=[] , mark=True):
        if mark:
            conn = self.pg_conn()
            if conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                try:
                    if pg_action:
                        action = pg_action[0].lower()
                        pg_table_name = pg_action[1]
                        current_date = time.strftime('%Y/%m/%d', time.gmtime())
                        if action == 'insert':
                            sql = 'insert into ' + pg_table_name + ' ' + str(tuple(object_params_dict.keys())).replace('\'', '') + ' values ' + str(tuple(object_params_dict.values())).replace('u\'', '\'')
                            print(sql)
                            cur.execute(sql)
                            conn.commit()

                        elif action in ['select', 'delete', 'update', 'truncate']:
                            if action == 'update':
                                column_value = '%s=$$%s$$' % (pg_action[2], object_params_dict[pg_action[2]])
                                del object_params_dict[pg_action[2]]

                            condition = ''
                            if object_params_dict:
                                for i in range(len(object_params_dict)):
                                    condition += '%s=$$%s$$' % (object_params_dict.keys()[i], object_params_dict.values()[i])
                                    if i + 1 != len(object_params_dict):
                                        condition += ' and '

                            if action == 'select':
                                sql = 'select * from ' + pg_table_name + ' where ' + condition
                            elif action == 'delete':
                                sql = 'delete from ' + pg_table_name + ' where ' + condition
                            elif action == 'update':
                                if condition:
                                    sql = 'update ' + pg_table_name + ' set ' + column_value + ' where ' + condition
                                else:
                                    sql = 'update ' + pg_table_name + ' set ' + column_value
                            elif action == 'truncate':
                                sql = 'truncate ' + pg_table_name
                            
                            cur.execute(sql)

                            if action == 'select':
                                rows = cur.fetchall()
                                if rows:
                                    return rows
                                else:
                                    return False
                            else:
                                conn.commit()
                        else:
                            return self.pg_extrs(conn, cur, object_params_dict, pg_action)
                    #print(sql)

                except psycopg2.Error as a:
                    print(a)
                finally:
                    conn.close()
