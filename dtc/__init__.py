#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: dtc/__init__.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-08 11:12
#########################################################################


from __future__ import absolute_import

# stdlib
import os
import sys
from collections import namedtuple

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_path)

version_info_t = namedtuple(
	'version_info_t', ('major', 'minor', 'micro', 'releaselevel', 'serial'),
)

SERIES = 'Commander'
VERSION = version_info_t(1, 0, 0, '', '')
__version__ = '{0.major}.{0.minor}.{0.micro}{0.releaselevel}'.format(VERSION)
__author__ = 'Tommy'
__contact__ = 'tm707167666@gmail.com'
__homepage__ = ''
__docformat__ = 'restructuredtext'

from .tasks import async, async_chain, result, queue_size
from .cluster import Cluster
from .status import Stat
from .brokers import get_broker
__all__ = ['cluster', 'tasks']

VERSION_BANNER = '{0} ({1})'.format(__version__, SERIES)
