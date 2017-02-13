#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: brokers/__init__.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-07 23:05
#########################################################################


# redis
import redis
# dtc
from dtc.conf import Conf


class Broker(object):

    def __init__(self, list_key=Conf.PREFIX):
        self.connection = self.get_connection(list_key)
        self.list_key = list_key
        self.cache = self.get_cache()
        self._info = None

    def enqueue(self, task_id, task, priority):
        """
        Puts a task onto the queue
        :type task_id: str
        :type task: str
        :type priority: int
        """
        pass

    def dequeue(self):
        """
        Gets a task from the queue
        :return: tupe with task id and task message
        """
        pass

    def queue_size(self):
        """
        :return: the amount of tasks in the queue
        """
        pass

    def lock_size(self):
        """
        :return: the number of tasks currently awaiting acknowledgement
        """
        pass

    def delete_queue(self):
        """
        Deletes the queue from the broker
        """
        pass

    def purge_queue(self):
        """
        Purges the queue of any tasks
        """
        pass

    def delete(self, task_id):
        """
        Deletes a task from the queue
        :param task_id: the id of the task
        """
        pass

    def acknowledge(self, task_id):
        """
        Acknowledges completion of the task and removes it from the queue
        :param task_id: the id of the task
        """
        pass

    def fail(self, task_id):
        """
        Fails a task message
        :param task_id:
        :return:
        """
        pass

    def ping(self):
        """
        Checks whether the broker connection is avalilable
        :rtype: bool
        """
        pass

    def info(self):
        """
        Shows the broker type
        """
        return self._info

    def set_stat(self, key, value, timeout):
        """
        Saves a cluster statistic to the cache provider
        :type key: str
        :type value: str
        :type timeout: int
        """
        if not self.cache:
            return
        key_list = self.cache.get(Conf.Z_STAT, [])
        if key not in key_list:
            key_list.append(key)
        self.cache.set(Conf.Z_STAT, key_list)
        return self.cache.set(key, value, timeput)

    def get_stat(self, key):
        """
        Gets a cluster statistic from the cache provider
        :type key: str
        :return: a cluster Stat
        """
        if not self.cache:
            return
        return self.cache.get(key)

    def get_stats(self, pattern):
        """
        Returns a list of all cluster stats from the cache provider
        :type pattern: str
        :return: a list of Stats
        """
        if not self.cache:
            return
        key_list = self.cache.get(Conf.Z_STAT)
        if not key_list or len(key_list) == 0:
            return []
        stats = []
        for key in key_list:
            stat = self.cache.get(key)
            if stat:
                stats.append(stat)
            else:
                key_list.remove(key)
        self.cache.set(Conf.Z_STAT, key_list)
        return stats

    @staticmethod
    def get_cache():
        """
        Gets the current cache provider
        :return: a cache provider
        """
        return redis.StrictRedis(**Conf.CACHE_REDIS)

    @staticmethod
    def get_connection(list_key=Conf.PREFIX):
        """
        Gets a connection to the broker
        :param list_key: Optional queue name
        :return: a broker connection
        """
        return 0

def get_broker(key=Conf.PREFIX):
    """
    Gets the configured broker type
    :param list_key: optional queue name
    :type list_key: str
    :return:
    """
    from brokers import redis_broker
    return redis_broker.Redis(key=key)
