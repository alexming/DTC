#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: config.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-12 00:31
#########################################################################


# stdlib
import os
import signal
import logging
from multiprocessing import cpu_count, Queue
try:
    import psutil
except ImportError:
    psutil = None
# config
from config import settings, BaseConf


class Conf(BaseConf):
    """
    Configuration class
    """
    try:
        conf = settings.DTC_CLUSTER
    except AttributeError:
        conf = {}
    # Redis server configuration. Fllows standard redis keywords
    REDIS = conf.get('redis', {})

    # Name of the cluster or site
    PREFIX = conf.get('name', 'default')

    # Log output level
    LOG_LEVEL = conf.get('log_level', 'INFO')

    # Disable the scheduler
    SCHEDULER = conf.get('scheduler', True)

    # Name of the redis node
    #TASK_UNIQUE_QUEUE_NODE = settings.REDIS_TASK_UNI_SOURCE

    # Number of workers in the pool. Default is cpu count if implemented, otherwise 4.
    WORKERS = conf.get('workers', False)
    if not WORKERS:
        try:
            WORKERS = cpu_count()
            # in rare cases this might fail
        except NotImplementedError:
            # try psutil
            if psutil:
                WORKERS = psutil.cpu_count() or 4
            else:
                # sensible default
                WORKERS = 4

    # Number of threads in pur worker. Default 5.
    THREADERS = conf.get('threads', False)
    if not THREADERS:
        THREADERS = 5

    # Maximum number of tasks that each cluster can work on
    QUEUE_LIMIT = conf.get('queue_limit', (int(WORKERS) ** 2) * int(THREADERS))

    # Sets compression of redis packages
    COMPRESSED = conf.get('compress', False)

    # Number of tasks each worker can handle before it gets recycled. Useful for releasing memory
    RECYCLE = conf.get('recycle', 500)

    # Number of seconds to wait for a worker to finish.
    TIMEOUT = conf.get('timeout', None)

    # Sets the number of processors for each worker, defaults to all.
    CPU_AFFINITY = conf.get('cpu_affinity', 0)

    # Use the cache as result backend. Can be 'True'
    CACHED = conf.get('cached', False)

    # Cache Redis server configuration.
    CACHE_REDIS = conf.get('cache_redis', {})

    # The priority of the task
    PRIORITY = conf.get('priority', 10)

    # The number of messages for each process to refetch
    PREFETCH = conf.get('prefetch', 1)

    # Broker
    BROKER_URL = conf.get('broker_url', None)

    # Backend
    BACKEND_URL = conf.get('backend', None)

    # sync task
    SYNC = False

    # The redis stats key
    Q_STAT = 'dtc:{}:cluster'.format(PREFIX)

    # OSX doesn't implement qsize because of missing sem_getvalue()
    try:
        QSIZE = Queue().qsize() == 0
    except NotImplementedError:
        QSIZE = False

    # to manage workarounds during testing
    TESTING = conf.get('testing', False)

# logger
logger = logging.getLogger(Conf.APPNAME)

# get parent pid compatibility
def get_ppid():
    if hasattr(os, 'getppid'):
        return os.getppid()
    elif psutil:
        return psutil.Process(os.getpid()).ppid()
    else:
        raise OSError('Your OS does not support `os.getppid`. Please install `psutil` as an alternative provider.')
