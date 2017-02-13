#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: settings.zpb.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-07 18:58
#########################################################################


APPNAME = 'zpb'

ZPB = {
    'database--': {
        'dialect': 'mysql',
        'driver': 'mysqldb',
        'username': 'sync_site_user',
        'password': 'ffd#@FDAS@23da2',
        'host': '119.29.154.182',
        'port': 3306,
        'database': 'helper_db',
        'charset': 'utf8'
    },
    'database': {
        'dialect': 'mysql',
        'driver': 'mysqldb',
        'username': 'helper_user',
        'password': 'fas@kd$29l@k2l',
        'host': '61.144.244.121',
        'port': 3306,
        'database': 'hr_helper',
        'charset': 'utf8'
    },
    'redis-nodes':{
        'login_cache_0': {'host': '127.0.0.1', 'port': 6379, 'db': 0},
        'dict_cache_1': {'host': '127.0.0.1', 'port': 6379, 'db': 1},
        'dict_cache_2': {'host': '127.0.0.1', 'port': 6379, 'db': 2},
        'task_resume': {'host': '127.0.0.1', 'port': 6379, 'db': 3},
        'task_mail': {'host': '127.0.0.1', 'port': 6379, 'db': 4},
        'task_uni_queue': {'host': '127.0.0.1', 'port': 6379, 'db': 5},
    },
    'httpserver':{
        'port': 8700
    },
    'ygys':{
        'soapuri': 'http://61.144.244.103:8700/ResumeService.asmx?wsdl',
        'username': 'none',
        'password': 'none',
        # 'soapuri': 'http://service.ygys.net/ResumeService.asmx?wsdl',
        # 'username': 'u100049',
        # 'password': 'PVBsGxfoAZ4=',
        'timeout': 90,
        'debug': False
    },
    'dama2':{
        'damauri': 'http://api.dama2.com:7766/app',
        # 'damauri': 'http://117.28.242.136:7766/app',
        'appid': 34425,
        'appkey': '958f6f5a408814c87703ba8a76fea95f',
        'username': '707167666',
        'passwd': 'dama2'
    },
    'proxy': {
        # 'host': '192.168.1.107',
        # 'port': 8888
    },
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 '\
        '(KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36',
}

DTC_CLUSTER = {
    'name': 'master',
    'cpu_affinity': 1,
    'threads': 10,
    'redis': {'host': '127.0.0.1', 'port': 6379, 'db': 5},
    'cache_redis':{'host': '127.0.0.1', 'port': 6379, 'db': 5},
    'cached': True,
    'prefetch': 4,
    'broker': 'redis://127.0.0.1:6379/5',
    'backend': 'redis://127.0.0.1:6379/5'
}
