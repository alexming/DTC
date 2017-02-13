#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: utils/signing.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-06 19:27
#########################################################################


try:
    import cPickle as pickle
except:
    import pickle

BadSerializer = pickle.UnpicklingError

class PickleSerializer(object):

    """Simple wrapper around Pickle for signing.dumps and signing.loads."""

    @staticmethod
    def dumps(obj):
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def loads(data):
        return pickle.loads(data)
