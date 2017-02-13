#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/cluster.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-07 22:59
#########################################################################


# stdlib
import signal
import socket
import importlib
from time import sleep
from datetime import datetime
from multiprocessing import Queue, Event, Process, Value, current_process
# local
import tasks
import signing
# dtc
from dtc.conf import Conf, logger, psutil, get_ppid
from dtc.status import Stat, Status
from dtc.brokers import get_broker
from dtc.threadpool import ThreadPool, WorkRequest


class Cluster(object):
    def __init__(self, broker=None):
        self.broker = broker or get_broker()
        self.sentinel = None
        self.stop_event = None
        self.start_event = None
        self.pid = current_process().pid
        self.host = socket.gethostname()
        self.timeout = Conf.TIMEOUT
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)

    def start(self):
        # start Sentinel
        self.stop_event = Event()
        self.start_event = Event()
        self.sentinel = Process(target=Sentinel, args=(self.stop_event, self.start_event, self.broker, self.timeout))
        self.sentinel.start()
        logger.info('DTC-Cluster-{} starting.'.format(self.pid))
        while not self.start_event.is_set():
            sleep(0.1)
        return self.pid

    def stop(self):
        if not self.sentinel.is_alive():
            return False
        logger.info('DTC-Cluster-{} stopping.'.format(self.pid))
        self.stop_event.set()
        self.sentinel.join()
        logger.info('DTC-Cluster-{} has stopped.'.format(self.pid))
        self.start_event = None
        self.stop_event = None
        return True

    def sig_handler(self, signum, frame):
        logger.warning('{} got signal {}'.format(current_process().name, Conf.SIGNAL_NAMES.get(signum, 'UNKNOWN')))
        self.stop()

    @property
    def stat(self):
        if self.sentinel:
            return Stat.get(self.pid)
        return Status(self.pid)

    @property
    def is_starting(self):
        return self.stop_event and self.start_event and not self.start_event.is_set()

    @property
    def is_running(self):
        return self.stop_event and self.start_event and self.start_event.is_set()

    @property
    def is_stopping(self):
        return self.stop_event and self.start_event and self.start_event.is_set() and self.stop_event.is_set()

    @property
    def has_stopped(self):
        return self.start_event is None and self.stop_event is None and self.sentinel


class Sentinel(object):
    def __init__(self, stop_event, start_event, broker=None, timeout=Conf.TIMEOUT, start=True):
        # Make sure we catch signals for the pool
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        self.pid = current_process().pid
        self.parent_pid = get_ppid()
        self.name = current_process().name
        self.broker = broker or get_broker()
        self.reincarnations = 0
        self.tob = datetime.now()
        self.stop_event = stop_event
        self.start_event = start_event
        self.pool_size = Conf.WORKERS
        self.pool = []
        self.timeout = timeout
        self.task_queue = Queue(maxsize=Conf.QUEUE_LIMIT) if Conf.QUEUE_LIMIT else Queue()
        self.result_queue = Queue()
        self.event_out = Event()
        self.monitor = None
        self.pusher = None
        if start:
            self.start()

    def start(self):
        self.broker.ping()
        self.spawn_cluster()
        self.guard()

    def status(self):
        if not self.start_event.is_set() and not self.stop_event.is_set():
            return Conf.STARTING
        elif self.start_event.is_set() and not self.stop_event.is_set():
            if self.result_queue.empty() and self.task_queue.empty():
                return Conf.IDLE
        elif self.start_event.is_set() and self.stop_event.is_set():
            if self.monitor.is_alive() or self.pusher.is_alive() or len(self.pool) > 0:
                return Conf.STOPPING
            return Conf.STOPPED

    def spawn_process(self, target, *args):
        """
        :type target: function or class
        """
        p = Process(target=target, args=args)
        p.daemon = True
        if target == worker:
            p.timer = args[2]
            self.pool.append(p)
        p.start()
        return p

    def spawn_pusher(self):
        return self.spawn_process(pusher, self.task_queue, self.event_out, self.broker)

    def spawn_worker(self):
        self.spawn_process(worker, self.task_queue, self.result_queue, Value('f', -1), self.timeout)

    def spawn_monitor(self):
        return self.spawn_process(monitor, self.result_queue, self.broker)

    def reincarnate(self, process):
        """
        :param process: the process to reincarnate
        :type process: Process or None
        """
        if process == self.monitor:
            self.monitor = self.spawn_monitor()
            logger.error('reincarnated monitor {} after sudden death'.format(process.name))
        elif process == self.pusher:
            self.pusher = self.spawn_pusher()
            logger.error('reincarnated pusher {} after sudden death'.format(process.name))
        else:
            self.pool.remove(process)
            self.spawn_worker()
            if self.timeout and int(process.timer.value) == 0:
                # only need to terminate on timeout, otherwise we risk destabilizing the queues
                process.terminate()
                logger.warning('reincarnated worker {} after timeout'.format(process.name))
            elif int(process.timer.value) == -2:
                logger.info('recycled worker {}'.format(process.name))
            else:
                logger.warning('reincarnated worker {} after death with timer value {}'.format(process.name, int(process.timer.value)))
        self.reincarnations += 1

    def spawn_cluster(self):
        self.pool = []
        Stat(self).save()
        # spawn worker pool
        for __ in range(self.pool_size):
            self.spawn_worker()
        # spawn auxiliary
        self.monitor = self.spawn_monitor()
        self.pusher = self.spawn_pusher()
        # set worker cpu affinity if needed
        if psutil and Conf.CPU_AFFINITY:
            set_cpu_affinity(Conf.CPU_AFFINITY, [p.pid for p in self.pool])

    def guard(self):
        logger.info('{} guarding cluster at {}'.format(current_process().name, self.pid))
        self.start_event.set()
        Stat(self).save()
        logger.info('DTC-Cluster-{} running.'.format(self.parent_pid))
        #scheduler(broker=self.broker)
        counter = 0
        cycle = 0.5 # guard loop sleep in seconds
        # guard loop. Runs at least once
        while not self.stop_event.is_set() or not counter:
            # Check Workers
            for p in self.pool:
                # Are you alive?
                if not p.is_alive() or (self.timeout and p.timer.value == 0):
                    self.reincarnate(p)
                    continue
                # Decrement timer if work is beging done
                if self.timeout and p.timer.value > 0:
                    p.timer.value -= cycle
            # Check Monitor
            if not self.monitor.is_alive():
                self.reincarnate(self.monitor)
            # Check Pusher
            if not self.pusher.is_alive():
                self.reincarnate(self.pusher)
            # Call scheduler once aminute (or so)
            counter += cycle
            if counter == 30 and Conf.SCHEDULER:
                counter = 0
                #scheduler(broker=self.broker)
            # Save current status
            Stat(self).save()
            sleep(cycle)
        self.stop()

    def stop(self):
        Stat(self).save()
        name = current_process().name
        logger.info('{} stopping cluster processes'.format(name))
        # Stopping pusher
        self.event_out.set()
        # Wait for it to stop
        while self.pusher.is_alive():
            sleep(0.1)
            Stat(self).save()
        # Put poison pills in the queue
        for __ in range(len(self.pool)):
            self.task_queue.put('STOP')
        self.task_queue.close()
        # Wait for the task queue to empty
        self.task_queue.join_thread()
        # Wait for all the workers to exit
        while len(self.pool):
            for p in self.pool:
                if not p.is_alive():
                    self.pool.remove(p)
            sleep(0.1)
            Stat(self).save()
        # Finally stop the monitor
        self.result_queue.put('STOP')
        self.result_queue.close()
        # Wait for the result queue to empty
        self.result_queue.join_thread()
        logger.info('{} Waiting for the monitor.'.format(name))
        # Wait for everything to close or timeout
        count = 0
        if not self.timeout:
            self.timeout = 30
        while self.status() == Conf.STOPPING and count < self.timeout * 10:
            sleep(0.1)
            Stat(self).save()
            count += 1
        # Final status
        Stat(self).save()


def pusher(task_queue, event, broker=None):
    if not broker:
        broker = get_broker()
    name = current_process().name
    pid = current_process().pid
    logger.info('{} pushing tasks at {}'.format(name, pid))
    while 1:
        try:
            task_set = broker.dequeue()
        except BaseException as e:
            logger.error(e)
            # broker probably crashed. Let the sentinel handle it.
            sleep(10)
            break
        if task_set:
            for task in task_set:
                try:
                    task = signing.PickleSerializer.loads(task)
                except (TypeError, KeyError, signing.BadSerializer):
                    logger.error(e)
                    continue
                task_queue.put(task)
            logger.debug('queueing from {}'.format(broker.list_key))
        else:
            sleep(.1)
        if event.is_set():
            break
    logger.info('{} stopped pushing tasks'.format(name))


def worker(task_queue, result_queue, timer, timeout=Conf.TIMEOUT):
    """
    Takes a task from the task queue, tries to execute it and puts the result back in the result queue
    :type task_queue: multiprocessing.Queue
    :type result_queue: multiprocessing.Queue
    :type timer: multiprocessing.Value
    """
    name = current_process().name
    logger.info('{} ready for work at {}'.format(name, current_process().pid))
    pool = ThreadPool(Conf.THREADERS)
    task_count = 0
    # 任务回调
    def worker_callback(request, response):
        task = request.task
        result = (response, not request.exception)
        # Process result
        task['result'] = result[0]
        task['success'] = result[1]
        task['stopped'] = datetime.now()
        result_queue.put(task)
        timer.value = -1 # Idle
        # Recycle
        if task_count == Conf.RECYCLE:
            timer.value = -2 # Recycled
            task_queue.put('STOP')
    # 任务分发
    for task in iter(task_queue.get, 'STOP'):
        result = None
        timer.value = -1 #Idle
        task_count += 1
        # Get the function from the task
        logger.debug('{} processing [{}]'.format(name, task['name']))
        f = task['func']
        # if it's not an instance try to get it from the string
        if not callable(task['func']):
            try:
                module, func = f.rsplit('.', 1)
                m = importlib.import_module(module)
                f = getattr(m, func)
            except (ValueError, ImportError, AttributeError) as e:
                result = (e, False)
        # We're still going
        if not result:
            # execute the payload
            timer.value = task['kwargs'].pop('timeout', timeout or 0) # Busy
            # res = f(*task['args'], **task['kwargs'])
            # result = (res, True)
            request = WorkRequest(f, args=task['args'], kwargs=task['kwargs'], callback=worker_callback)
            request.task = task
            pool.putRequest(request)
        else:
            # Process result
            task['result'] = result[0]
            task['success'] = result[1]
            task['stopped'] = datetime.now()
            result_queue.put(task)
            timer.value = -1 # Idle
    pool.stop()
    logger.info('{} stopped doing work'.format(name))


def monitor(result_queue, broker=None):
    """
    Gets finished tasks from the result queue and saves them to DB or Cache
    :type result_queue: multiprocessing.Queue
    """
    if not broker:
        broker = get_broker()
    name = current_process().name
    pid = current_process().pid
    logger.info('{} monitoring at {}'.format(name, pid))
    for task in iter(result_queue.get, 'STOP'):
        ackid = task.get('id', None)
        if ackid:
            broker.acknowledge(ackid)
        # save the result
        save_cached(task, broker)
        # log the result
        if task['success']:
            logger.debug("Processed [{}]".format(task['name']))
        else:
            logger.error("Failed [{}] - {}".format(task['name'], task['result']))
    logger.info("{} stopped monitoring results".format(name))


def save_cached(task, broker):
    task_key = '{}:{}'.format(broker.list_key, task['id'])
    try:
        group = task.get('group', None)
        # if it's a group append to the group list
        if group:
            if task.get('save', False):
                group_key = '{}:{}:keys'.format(broker.list_key, group)
                group_list = broker.cache.get(group_key)
                if group_list:
                    group_list = signing.PickleSerializer.loads(group_list)
                else:
                    group_list = []
                # save the group list
                group_list.append(task_key)
                broker.cache.set(group_key, signing.PickleSerializer.dumps(group_list))
            # async next in a chain
            if task.get('chain', None) and task['success']:
                tasks.async_chain(task['chain'], group=group, cached=task['cached'],
                                  sync=task['sync'], priority=task['priority'], broker=broker)
        # save the task
        if task.get('save', False):
            broker.cache.set(task_key, signing.PickleSerializer.dumps(task))
    except Exception as e:
        logger.error(e)


def set_cpu_affinity(n, process_ids, actual=not Conf.TESTING):
    """
    Sets the cpu affinity for the supplied processes.
    Requires the optional psutil module.
    :param int n: affinity
    :param list process_ids: a list of pids
    :param bool actual: Test workaround for Travis not supporting cpu affinity
    """
    # check if we have the psutil module
    if not psutil:
        logger.warning('Skipping cpu affnity bacause psutil was not found.')
        return
    # check if the platform supports cpu_affinity
    if actual and not hasattr(psutil.Process(process_ids[0]), 'cpu_affinity'):
        logger.warning('Faking cpu affinity because it is not supported on this platform')
        actual = False
    # get the available processors
    cpu_list = list(range(psutil.cpu_count()))
    # affinities of 0 or gte cpu_count, equals to no affinity
    if not n or n >= len(cpu_list):
        return
    # spread the workers over the available processors.
    index = 0
    for pid in process_ids:
        affinity = []
        for k in range(n):
            if index == len(cpu_list):
                index = 0
            affinity.append(cpu_list[index])
            index += 1
        if psutil.pid_exists(pid):
            p = psutil.Process(pid)
            if actual:
                p.cpu_affinity(affinity)
            logger.info('{} will use cpu {}'.format(pid, affinity))
