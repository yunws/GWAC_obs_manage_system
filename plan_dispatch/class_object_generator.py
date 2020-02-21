#!/usr/bin/python
# -*- coding: utf-8 -*-
#author: Xiao Yujie
#update: 2019-05-13
#version: 0.0.4

# from __future__ import unicode_literals
import sys, os, time, textwrap
import psycopg2, codecs
import colorama
from coords_trans import *

class object_generator:
    colorama.init()

    def __init__(self, *args, **kwargs):

        self.TypeError = []
        self.ValueError = []
        self.OtherError = []

        # self.object_params = {$observer:[$object_params_dict, ], }
        # $object_params_dict = {'obj_name':$obj_name, 'objra':$objra, 'objdec':$objdec, 'expdur':$expdur, 'filter':$filter, 'frmcnt':$frmcnt[, 'priority']}
        # self.object_params = {}

        # self.object_params = [{'obj_name':$obj_name, 'objra':$objra, 'objdec':$objdec, 'expdur':$expdur, 'filter':$filter, 'frmcnt':$frmcnt[, 'priority']}, ]
        self.object_params = []

        # get program path
        self.iswindows = True if sys.platform == 'win32' else False
        self.__path__ = ''
        if self.iswindows:
            for i in os.path.realpath(__file__).split('\\')[:-1]:
                self.__path__ += i + '/'
        else:
            for i in os.path.realpath(__file__).split('/')[:-1]:
                self.__path__ += '/' + i
            self.__path__ += '/'

        if len(args) > 2:
            raise TypeError(
                "object_generator() takes at most 2 arguments (%d given). Usege: object_generator([observer[, priority[, **kwargs]]] \
                \n args: %s" % (len(args),args)
                )

        kwargs['init'] = True
        self.set_pg(**kwargs)

        if not ('skip_confirm' in kwargs and kwargs['skip_confirm']):
            self.generator_setting(*args, **kwargs)

    def generator_setting(self, *args, **kwargs):
        if not args and not kwargs:
            print self.set_pg()
            print self.set_observer()

        # set observer and priority
        elif not args:
            if 'observer' not in kwargs:
                kwargs['init'] = True
                self.set_observer(**kwargs)
            else:
                self.set_observer(kwargs['observer'], **kwargs)

            self.priority = kwargs['priority'] if 'priority' in kwargs else '20'

        elif len(args) == 1 and 'observer' not in kwargs:
            self.set_observer(*args, **kwargs)
            self.priority = kwargs['priority'] if 'priority' in kwargs else '20'

        elif len(args) == 2 and ('observer' not in kwargs) and ('priority' not in kwargs):
            args1, args2 = args

            if args2.isdigit() or type(args2) == type(int()):
                args2 = str(args2)
            elif not args2.split() or 'null' in args2.lower():
                args2 = '20'
            else:
                raise TypeError(
                    "priority must be digit or null. Usege: generator_setting([observer[, priority[, init=True[, **kwargs]]]])")

            self.set_observer(args1, **kwargs)
            self.priority = args2
        else:
            raise TypeError(
                "generator_setting() takes at most 2 arguments (%d given). Usege: generator_setting([observer[, priority[, init=True[, **kwargs]]]])" % len(args))

    def set_observer(self, *args, **kwargs):
        '''
        Try match observer from observer.conf. IF observer in observer.conf,
        set self.observer as observer; else confirm if adding observer into observer.conf,
        and set self.observer as observer.

        '''

        self.observer_conf = self.__path__ + 'conf/observer.conf'

        action = kwargs['action'] if 'action' in kwargs else ''
        skip_confirm = kwargs['skip_confirm'] if 'skip_confirm' in kwargs else False

        if not args and not kwargs:
            return (self.observer, self.observer_conf)

        elif not args and 'init' in kwargs and kwargs['init']:
            observer = ''

        elif len(args) == 1:
            observer = args[0]

        else:
            raise TypeError(
                "set_observer() takes at most 1 argument (%d given). Usege: set_observer([observer[, action=''[, init=True[, **kwargs]]]])" % len(args))       

        key_note = dict(
            observer=u'观测者', 
            organizaion=u'所属单位（项目组）', 
            dispensing=u'分发方式', 
            host='host', 
            user='user', 
            password='password', 
            path=u'分发路径', 
            note=u'备注')

        current_import_observer = dict(observer=observer, organizaion='GWAC', dispensing='ftp', host='0', user='0', password='0', path='0', note='')

        observer_list = []

        observer_conf = codecs.open(self.observer_conf, 'a+', 'utf-8')
        conf_lines = observer_conf.readlines()
        observer_conf.close()

        if not ('init' in kwargs and not kwargs['init']):
            if len(conf_lines):
                self.observer_conf_dict = {}
                for i in range(len(conf_lines)):
                    line = conf_lines[i]
                    if line and ('#' not in line):
                        observer_list.append(line.split()[0])
                        if observer == line.split()[0]:
                            if skip_confirm:
                                confirm = 'y'
                            else:
                                print u'\n将要设置观测者：', observer
                                print u'\n输入：y 以确认，其他以编辑观测者\n>>> '
                                confirm = raw_input()
                            if confirm in ['y', 'Y'] :
                                self.observer = observer
                                print u'成功设置 %s 为观测者\n' % self.observer
                                
                                ###
                                # self.object_params[self.observer] = []
                                
                                return self.observer

                        self.observer_conf_dict[str(i)] = line

        if not action:
            print u'\n#\t序号\t%s' % conf_lines[0][2:],
            for key in sorted(self.observer_conf_dict.keys()):
                print_line = ('#\t%s'+ '\t%s'*len(self.observer_conf_dict[key].split())) % (tuple(key) + tuple(self.observer_conf_dict[key].split()))
                print print_line
            if observer not in observer_list:
                print u'\n当前输入观测者: %s 与配置文件不匹配。\n' % observer
            print u'输入 \033[1;31m序号\033[0m 可选定配置文件中的观测者。若要以命令行添加观测者，请输入 c ；图形化界面编辑配置文件请输入 g'
            action = raw_input('\n>>> ')

        if action in self.observer_conf_dict:
            self.observer = self.observer_conf_dict[action].split()[0]
            print u'成功设置 %s 为观测者\n' % self.observer

            ####
            # self.object_params[self.observer] = []
            return self.observer

        elif action in ['c', 'C']:
            index = conf_lines[0][2:].split()
            write_line = ''
            for i in index:
                key = key_note.keys()[key_note.values().index(i)]
                import_params = raw_input(u'编辑%s (当前:%s) : ' % (key, current_import_observer[key]))
                current_import_observer[key] = import_params if import_params else current_import_observer[key]
                write_line += current_import_observer[key] + '\t'
            
            print '\n%s' % conf_lines[0][2:]
            print write_line
            print u'输入：y 写入配置文件中\n>>> '
            confirm = raw_input()
            if confirm in ['y', 'Y'] :
                observer_conf = codecs.open(self.observer_conf, 'a+', 'utf-8')
                observer_conf.write('\n' + write_line)
                observer_conf.close()

        elif action in ['g', 'G']:
            if self.iswindows:
                editer = 'notepad'
            else:
                editer = 'gedit'
            observer_conf.close()
            os.popen(editer + ' ' + self.observer_conf)

        else:
            print u'输入有误，重新输入'
            action = raw_input('\n>>> ')
            return self.set_observer(current_import_observer['observer'], action=action, init=False)
        
        return self.set_observer(current_import_observer['observer'], action='')

    def set_pg(self, **kwargs):
        if 'init' in kwargs and kwargs['init']:
            #self.pg_host = '10.0.10.236'
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

        self.key_in_pgdb = self.get_keys_in_pgdb()

        return (self.pg_host, self.pg_port, self.pg_user, self.pg_passwd, self.pg_db)

    def pg_conn(self):
        try:
            conn = psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_user, password=self.pg_passwd, database=self.pg_db)
            return conn
        except psycopg2.Error, a:
            print a#, u'\n数据库连接异常：', (self.pg_host, self.pg_port, self.pg_user, self.pg_passwd, self.pg_db)
            return False

    def get_keys_in_pgdb(self):
        conn = self.pg_conn()
        if conn:
            cur = conn.cursor()
            try:
                sql = "select column_name from information_schema.columns where table_schema='public' and table_name='object_list_all'"
                cur.execute(sql)
                rows = cur.fetchall()
                _key_in_pgdb = []
                for i in rows:
                    _key_in_pgdb.append(i[0])
                return _key_in_pgdb
            except psycopg2.Error, a:
                print a
                return False
            finally:
                conn.close()  

    def pg_process(self, object_params_dict, pg_action='' ,mark=True):
        if mark:
            conn = self.pg_conn()
            if conn:
                cur = conn.cursor()
                try:
                    pg_action = pg_action.lower()
                    if pg_action and object_params_dict:
                        if pg_action == 'insert':
                            sql = 'insert into object_list_all ' + str(tuple(object_params_dict.keys())).replace('\'', '') + ' values ' + str(tuple(object_params_dict.values())).replace('u\'', '\'')
                            print sql
                            cur.execute(sql)
                            conn.commit()

                        elif pg_action in ['select', 'delete']:
                            condition = ''
                            for i in range(len(object_params_dict)):
                                condition += '%s=$$%s$$' % (object_params_dict.keys()[i], object_params_dict.values()[i])
                                if i + 1 != len(object_params_dict):
                                    condition += ' and '

                            if pg_action == 'select':
                                sql = 'select * from object_list_all where ' + condition
                            else:
                                sql = 'delete from object_list_all where ' + condition
                            
                            cur.execute(sql)

                            if pg_action == 'select':
                                rows = cur.fetchall()
                                if rows:
                                    return True
                                else:
                                    return False
                            else:
                                conn.commit()
                        else:
                            print 'pg_action error: aciton =', pg_action
                            sys.exit()
                    #print sql


                except psycopg2.Error, a:
                    print a
                finally:
                    conn.close()

    def pg_proc_object_params(self, pg_action=''):
        if self.TypeError or self.ValueError or self.OtherError:
            if self.TypeError:
                print 'obj TypeError: ', self.TypeError
            if self.ValueError:
                print 'obj ValueError: ', self.ValueError
            if self.OtherError:
                print 'obj OtherError: ', self.OtherError

            sys.exit()

        if self.object_params:
            pg_action = pg_action.lower()
            for object_params_dict in self.object_params:
                if pg_action == 'insert' and self.pg_process(object_params_dict, pg_action='select'):
                    print 'the line exist: ', object_params_dict
                    # print u'记录已存在：', object_params_dict
                    continue
                if pg_action == 'delete' and not self.pg_process(object_params_dict, pg_action='select'):
                    print 'the line do not exist: ', object_params_dict
                    # print u'记录不存在：', object_params_dict
                    continue

                self.pg_process(object_params_dict, pg_action ,mark=True)

                if pg_action == 'delete' and not self.pg_process(object_params_dict, pg_action='select'):
                    print 'delete: ', object_params_dict
                    # print u'成功删除记录：', object_params_dict

    def plan_filter(self, *args, **kwargs):
        if not args:
            self.object_params_append(**kwargs)
        elif len(args) == 1:
            plan = args[0]
            if not plan:
                self.object_params_append(**kwargs)
            elif type(plan) in [type('str'), type('str'.decode('utf-8'))]:
                plan = plan.encode('utf-8')
                if os.path.isfile(plan):
                    plan_lines = file(plan).readlines()
                    for plan_line in plan_lines:
                        self.object_params_append(plan_line, **kwargs)

                elif os.path.isdir(plan):
                    plan_files = os.listdir(plan)
                    for file_name in plan_files:
                        plan_file = plan + '/' + file_name
                        if os.path.isfile(plan_file):
                            self.plan_filter(plan_file, **kwargs)           

                else:
                    plan_split = plan.split()
                    if os.path.isfile(plan_split[0]):
                        plan_lines = file(plan_split[0]).readlines()
                        for plan_line in plan_lines:
                            self.object_params_append(plan_line, **kwargs)
                    elif os.path.isdir(plan_split[0]):
                        plan_files = os.listdir(plan_split[0])
                        for file_name in plan_files:
                            plan_file = plan_split[0] + '/' + file_name
                            if os.path.isfile(plan_file):
                                self.plan_filter(plan_file, **kwargs)
                    else:
                        self.object_params_append(plan, **kwargs)

            elif type(plan) == type(file(__file__)):
                plan_lines = plan.readlines()
                for plan_line in plan_lines:
                    self.object_params_append(plan_line, **kwargs)
        else:
            for arg in args:
                self.plan_filter(arg, **kwargs)

    def object_params_append(self, *args, **kwargs):
        # print args, kwargs

        if len(args) == 1:
            plan_line = args[0]
            if not plan_line:
                plan_line_list = ''
            elif '#' not in plan_line and plan_line.split():
                plan_line_list = plan_line.split()
            else:
                return
        elif not args:
            plan_line_list = ''
        else:
            for arg in args:
                return self.object_params_append(arg, **kwargs)
        #try:
        epoch = '2000'
        if len(plan_line_list) == 6:
            obj_name, objra, objdec, expdur, filtern, frmcnt = plan_line_list
        elif len(plan_line_list) == 7:
            obj_name, objra, objdec, epoch, expdur, filtern, frmcnt = plan_line_list
        else:
            if '=' in plan_line:
                for i in plan_line_list:
                    p_key, p_value = i.split('=')
                    kwargs[p_key] = p_value
            plan_line = ''
            if len(args):
                for i in args:
                    plan_line += i + ' ' 
            for i in kwargs:
                plan_line += '\t%s=%s' % (i, kwargs[i])
            if 'obj_name' in kwargs:
                obj_name = kwargs['obj_name']
            if 'objra' in kwargs:
                objra = kwargs['objra']
            if 'objdec' in kwargs:
                objdec = kwargs['objdec']
            if 'objepoch' in kwargs:
                epoch = kwargs['objepoch']
            if 'expdur' in kwargs:
                expdur = kwargs['expdur']
            if 'filter' in kwargs:
                filtern = kwargs['filter']
            if 'frmcnt' in kwargs:
                frmcnt = kwargs['frmcnt']
        if len(obj_name) > 20 :
            obj_name = obj_name[:20]
        objra = Ra_to_degree(objra)
        objdec = Dec_to_degree(objdec)
	"""
		for i in str(expdur).split('.'):
			if not i.isdigit():
				expdur_isdigit = False
				break
			expdur_isdigit = True"""
				
        if not (Ra_to_degree(objra) and Dec_to_degree(objdec) and str(frmcnt).isdigit()):# and expdur_isdigit):
            raise TypeError(
                "'objra, objdec, expdur, frmcnt' must be digit.")

        object_params_dict = dict(
            obj_name = obj_name,
            objra = objra,
            objdec = objdec,
            objepoch = epoch,
            expdur = expdur,
            filter = filtern,
            frmcnt = frmcnt,
            observer = self.observer if hasattr(self, 'observer') else 'test',

            # 新数据库必填项
            imgtype = 'object',
            mode = 'obeservation',
            date_beg = time.strftime('%Y/%m/%d', time.gmtime()),
            date_end = time.strftime('%Y/%m/%d', time.gmtime()),

            objerror = '0.0|0.0',
            group_id = 'XL002', # 001-->GWAC 002-->60 003-->30
            unit_id = '001' , # group_id == XL002: 001-->w60 002-->e60
            obs_type = 'ObA',
            obs_stra = 'pointing'
        )

        if hasattr(self, 'priority'):
            object_params_dict['priority'] = self.priority

        if kwargs:
            for key in kwargs:
                if key in self.key_in_pgdb:
                    object_params_dict[key] = kwargs[key]
        
        #self.object_params[self.observer].append(object_params_dict)
        self.object_params.append(object_params_dict)
        """except TypeError, a:
            print u'目标信息：', plan_line
            self.TypeError.append(plan_line)
            print '\n****************\nTypeError: ', a
            print u'请检查objra, objdec, expdur, frmcnt是否正确'
        except ValueError, a:
            print u'目标信息：', plan_line
            self.ValueError.append(plan_line)
            print '\n****************\nValueError:', a
            print u'请检查赤经、赤纬项是否正确'
        except:
            print u'目标信息：', plan_line
            self.OtherError.append(plan_line)            
            print '*' * 20 + '\n'
            print u'参数不足，请检查是否按如下顺序输入目标信息：\n   obj_name, objra, objdec, expdur, filter, frmcnt\n   obj_name, objra, objdec, objepoch, expdur, filter, frmcnt'"""

    def object_generate(self, *args, **kwargs):
        default_keys = ['observer', 'obj_name', 'objra', 'objdec', 'priority', 'filter', 'expdur', 'frmcnt']
        default_keys_flag = True
        if not args:
            for key in default_keys:
                if key not in kwargs.keys():
                    default_keys_flag = False
        if  default_keys_flag or ((args or kwargs) and args[0]):
            self.plan_filter(*args, **kwargs)
        else:
            _args = ''
            while not _args:
                print '*' * 20
                print u'缺少观测目标信息，请手动添加如下信息：\n      obj_name\tobjra\tobjdec\texpdur\tfiltern\tfrmcnt\n\n  如: grb\t30\t20\t15\tR\t5'
                print u'\n\n或输入观测目标文件路径， 如：  ./plan'
                _args = raw_input('\n>>> ')

            _args_list = _args.split()
            arg = ''
            kwargs = {}
            for i in _args_list:
                if '=' in i:
                    key, value = i.split('=')
                    kwargs[key.lower()] = value
                else:
                    arg += i + '\t'

            self.plan_filter(arg, **kwargs)

#if __name__ == "__main__":
    #test = object_generator('hanxuhui','1', skip_confirm=True)
    #test.set_observer('hanxuhui')
    #test.generator_setting()
    #test.set_observer('hanxuhui','2')
    #print test.set_observer('')
    #test.object_generate()
    #print test.object_params
    #test.pg_process({'observer':'hanxuhui'}, action='select')
    # print test.TypeError, test.ValueError, test.OtherError
    #test.pg_insert(insert_mark=True)
