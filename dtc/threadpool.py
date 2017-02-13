#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: dtc/threadpool.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-08 23:14
#########################################################################


# stdlib
import sys
import threading
import Queue
import traceback
# dtc
from dtc.conf import logger


# 自定义异常处理
class NoResponsePending(Exception):
    pass

class NoWorkerAvailable(Exception):
    pass

def _handle_thread_exception(request, exc_info):
    traceback.print_exception(*exc_info)


class WorkerThread(threading.Thread):

    def __init__(self, requestQueue, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.setDaemon(True)
        self._requestQueue = requestQueue
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        while 1:
            if self._dismissed.is_set():
                break
            try:
                # 阻塞式,超时2S
                request = self._requestQueue.get(block=True, timeout=1)
            except Queue.Empty:
                continue
            else:
                if self._dismissed.is_set():
                    self._requestQueue.put(request)
                    break
                try:
                    try:
                        response = request.callable(*request.args, **request.kwargs)
                    except BaseException as e:
                        request.exception = True
                        response = str(e)
                        if request.exc_callback:
                            request.exc_callback(request, sys.exc_info())
                finally:
                    if request.callback:
                        request.callback(request, response)

    def dismiss(self):
       self._dismissed.set()


class WorkRequest(object):

    def __init__(self, callable_, args=None, kwargs=None, requestID=None, callback=None, exc_callback=_handle_thread_exception):
        if requestID is None:
            self.requestID = id(self)
        else:
            try:
                self.requestID = hash(requestID)
            except TypeError:
                raise TypeError("requestId must be hashable")
        self.exception = False
        self.callback = callback
        self.exc_callback = exc_callback
        self.callable = callable_
        self.args = args or []
        self.kwargs = kwargs or {}

    def __str__(self):
        return "WorkRequest id=%s args=%r kwargs=%r exception=%s" % \
                (self.requestID, self.args, self.kwargs, self.exception)


class ThreadPool(object):

    def __init__(self, numOfWorkers, q_size=0, resq_size=0):
        self._requestQueue = Queue.Queue(q_size)
        self.workers = []
        self.dismissedWorkers = []
        self.workRequests = {}
        self.createWorkers(numOfWorkers)

    def createWorkers(self, numOfWorkers):
        for _ in xrange(numOfWorkers):
            self.workers.append(WorkerThread(self._requestQueue))

    def dismissWorkers(self, numOfWorkers, doJoin = False):
        dismiss_list = []
        for _ in xrange(numOfWorkers):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)
        if doJoin:
            for worker in dismiss_list:
                worker.join()
        else:
            self.dismissedWorkers.extend(dismiss_list)

    def joinAllDismissedWorkers(self):
        for worker in self.dismissedWorkers:
            worker.join()
        self.dismissedWorkers = []

    def putRequest(self, request, block = True, timeout=None):
        assert isinstance(request, WorkRequest)
        assert not getattr(request, 'exception', None)
        self._requestQueue.put(request, block, timeout)
        self.workRequests[request.requestID] = request

    def workersize(self):
        return len(self.workers)

    def stop(self):
        self.dismissWorkers(self.workersize(), True)
        self.joinAllDismissedWorkers()
