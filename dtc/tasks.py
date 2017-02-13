#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: dtc/tasks.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-08 11:16
#########################################################################

# stdlib
import time
from datetime import datetime
from multiprocessing import Queue, Value
# local
import signing
import cluster
# dtc
from dtc.conf import Conf, logger
from dtc.humanhash import uuid
from dtc.brokers import get_broker


def async(func, *args, **kwargs):
    """Queue a task for the cluster."""
    keywords = kwargs.copy()
    opt_keys = ('group', 'save', 'sync', 'cached', 'priority', 'chain', 'broker', 'uid')
    d_options = keywords.pop('d_options', None)
    # get an id
    tag = uuid()
    # build the task package
    task = {'id': tag[1],
            'name': tag[0],
            'func': func,
            'args': args}
    # push optionals
    for key in opt_keys:
        if d_options and key in d_options:
            task[key] = d_options['key']
        elif key in keywords:
            task[key] = keywords.pop(key)
    # broker
    broker = keywords.pop('broker', get_broker())
    # group
    if task.get('uid', False):
        task['id'] = task['uid']
    # overrides
    if 'cached' not in task and Conf.CACHED:
        task['cached'] = Conf.CACHED
    if 'sync' not in task and Conf.SYNC:
        task['sync'] = Conf.SYNC
    if 'priority' not in task or task['priority'] is None:
        task['priority'] = Conf.PRIORITY
    # finalize
    task['kwargs'] = keywords
    task['started'] = datetime.now()
    # sign it
    pack = signing.PickleSerializer.dumps(task)
    # sync
    if task.get('sync', False):
        return _sync(pack)
    # push it
    ret = broker.enqueue(task['id'], pack, task['priority'])
    logger.debug('Pushed {}'.format(task['id']))
    return ret, task['id']

def result(task_id, wait=0, cached=Conf.CACHED):
    """
    Return the result of the named task.
    :type task_id: str
    :param task_id: the task name
    :type wait: int
    :param wait: number of milliseconds to wait for a result
    :param bool cached: run this against the cache backend
    :return: the result object of this task
    :rtype: object
    """
    if cached:
        return result_cached(task_id, wait)
    start = time.time()
    while 1:
        r = None#Task.get_result(task_id)
        if r:
            return r
        if (time.time() - start) * 1000 >= wait >= 0:
            break
        time.sleep(0.01)

def result_cached(task_id, wait=0, broker=None):
    """
    Return the result from the cache backend
    """
    if not broker:
        broker = get_broker()
    start = time.time()
    while 1:
        r = broker.cache.get('{}:{}'.format(broker.list_key, task_id))
        if r:
            task = signing.PickleSerializer.loads(r)
            delete_cached(task_id, broker)
            return task['success'], task['result']
        if (time.time() - start) * 1000 >= wait >= 0:
            break
        time.sleep(0.01)

def result_group(group_id, failures=False, wait=0, count=None, cached=Conf.CACHED):
    """
    Return a list of results for a task group.
    :param str group_id: the group id
    :param bool failures: set to True to include failures
    :param int count: Block until there ara this many results in the group
    :param bool cached: run this against the cache backend
    :return: list or results
    """
    if cached:
        return result_group_cached(group_id, failures, wait, count)
    start = time.time()
    if count:
        while 1:
            if count_group(group_id) == count or wait and (time.time() - start) * 1000 >= wait >= 0:
                break
            tile.sleep(0.01)
    while 1:
        r = None#Task.get_result_group(group_id, failures)
        if r:
            return r
        if (time.time() - start) * 1000 >= wait >= 0:
            break
        time.sleep(0.01)

def result_group_cached(group_id, failures=False, wait=0, count=None, broker=None):
    """
    Return a list of results for a task group from the cache backend
    """
    if not broker:
        broker = get_broker()
    start = time.time()
    if count:
        while 1:
            if count_group(group_id) == count or wait and (time.time() - start) * 1000 >= wait >= 0:
                break
            tile.sleep(0.01)
    while 1:
        group_list = broker.cache.get('{}:{}:keys'.format(broker.list_key, group_id))
        if group_list:
            result_list = []
            for task_key in group_list:
                task = signing.SignedPackage.loads(broker.cache.get(task_key))
                if task['success'] or failures:
                    result_list.append(task['result'])
            return result_list
        if (time.time() - start) * 1000 >= wait >= 0:
            break
        time.sleep(0.01)

def count_group(group_id, failures=False, cached=Conf.CACHED):
    """
    Count the results in a group
    :param str group_id: the group id
    :param bool failures: Returns failure count if True
    :param bool cached
    :return: the number of tasks/results in a group
    :rtype: int
    """
    if cached:
        return count_group_cached(group_id, failures)
    return None#Task.get_group_count(group_id, failures)

def count_group_cached(group_id, failures=False, broker=None):
    """
    Count the results in a group in the cache backend
    """
    if not broker:
        broker = get_broker()
    group_list = broker.cache.get('{}:{}:keys'.format(broker.list_key, group_id))
    if group_list:
        if not failures:
            return len(failures)
        failure_count = 0
        for task_key in group_list:
            task = signing.SignedPackage.loads(broker.cache.get(task_key))
            if not task['success']:
                failure_count += 1
        return failure_count

def delete_group(group_id, tasks=False, cached=Conf.CACHED):
    """
    Delete a group
    :param str group_id: the group id
    :param bool tasks: If set to True this will also delete the group tasks.
    Otherwise just the group label is removed.
    :param bool cached
    :return:
    """
    if cached:
        return delete_group_cached(group_id)
    return None#Task.delete_group(group_id, tasks)

def delete_group_cached(group_id, broker=None):
    """
    Delete a group from the cache backend
    """
    if not broker:
        broker = get_broker()
    group_key = '{}:{}:keys'.format(broker.list_key, group_id)
    group_list = broker.cache.get(group_key)
    broker.cache.delete_many(group_list)
    broker.cache.delete(group_key)

def delete_cached(task_id, broker=None):
    """
    Delete a task from the cache backend
    """
    if not broker:
        broker = get_broker()
    broker.cache.delete('{}:{}'.format(broker.list_key, task_id))

def queue_size(broker=None):
    if not broker:
        broker = get_broker()
    return broker.queue_size()

def async_chain(chain, group=None, cached=Conf.CACHED, sync=Conf.SYNC, priority=Conf.PRIORITY, broker=None):
    """
    async a chain of tasks
    the chain must be in format [(func, (args), {kwargs}), (func, (args), {kwargs})]
    """
    if not group:
        group = uuid()[1]
    args = ()
    kwargs = {}
    task = chain.pop(0)
    if type(task) is not tuple:
        task = (task,)
    if len(task) > 1:
        args = task[1]
    if len(task) > 2:
        kwargs = task[2]
    kwargs['chain'] = chain
    kwargs['group'] = group
    kwargs['cached'] = cached
    kwargs['sync'] = sync
    kwargs['priority'] = priority
    async(task[0], *args, **kwargs)
    return True, group

def _sync(task):
    task_queue = Queue()
    result_queue = Queue()
    task = signing.SignedPackage.loads(pack)
    task_queue.put(task)
    task_queue.put('STOP')
    cluster.worker(task_queue, result_queue, Value('f', -1))
    result_queue.put('STOP')
    cluster.monitor(result_queue)
    return True, task['id']
