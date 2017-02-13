#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: brokers/redis_broker.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-07 23:29
#########################################################################


# redis
import redis
# dtc
from dtc.brokers import Broker
# from dtc.cache.rediscache import TQRedis
from dtc.conf import Conf, logger


class Redis(Broker):

    def __init__(self, key=Conf.PREFIX):
        super(Redis, self).__init__(list_key='dtc:{}:q'.format(key))
        self.unique_key = 'dtc:{}:u'.format(key)

    def enqueue(self, task_id, task, priority=10):
        if self.connection.sadd(self.unique_key, task_id):
            return self.connection.zadd(self.list_key, priority, task)

    def dequeue(self):
        ret = []
        tasks = self.connection.zrange(self.list_key, 0, Conf.PREFETCH - 1)
        if tasks:
            for task in tasks:
                if self.connection.zrem(self.list_key, task):
                    ret.append(task)
        return ret

    def queue_size(self):
        return self.connection.zcard(self.list_key)

    def delete_queue(self):
        return self.connection.delete(self.list_key)

    def purge_queue(self):
        return self.connection.zremrangebyrank(self.list_key, 0, -1)

    def acknowledge(self, task_id):
        self.connection.srem(self.unique_key, task_id)

    def ping(self):
        try:
            return self.connection.ping()
        except redis.ConnectionError as e:
            logger.error('Can not connect to Redis server.')
            raise e

    def info(self):
        if not self._info:
            info = self.connection.info('server')
            self._info = 'Redis {}'.format(info['redis_version'])
        return self._info

    def set_stat(self, key, value, timeout):
        self.connection.set(key, value, timeout)

    def get_stat(self, key):
        if self.connection.exists(key):
            return self.connection.get(key)

    def get_stats(self, pattern):
        keys = self.connection.keys(pattern=pattern)
        if keys:
            return self.connection.mget(keys)

    @staticmethod
    def get_connection(list_key=Conf.PREFIX):
        return redis.StrictRedis(**Conf.REDIS)
        # return TQRedis.GetRedis(Conf.TASK_UNIQUE_QUEUE_NODE)
