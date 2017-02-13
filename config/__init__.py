#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: conf/__init__.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-08 16:37
#########################################################################


# stdlib
import os
import time
import signal
import warnings
import importlib
import logging.config

ENVIRONMENT_VARIABLE = "SETTINGS_MODULE"

class BaseSettings(object):
    """
    Common logic for settings whether set by a module or by the user.
    """
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


class Settings(BaseSettings):

    def __init__(self, setting_module=None):
        if not setting_module:
            setting_module = os.environ.get(ENVIRONMENT_VARIABLE, 'settings')
        self.SETTINGS_MODULE = setting_module
        mod = importlib.import_module(self.SETTINGS_MODULE)

        tuple_settings = (

        )
        self._explicit_settings = set()
        for setting in dir(mod):
            if setting.isupper():
                setting_value = getattr(mod, setting)
                if (setting in tuple_settings and not isinstance(setting_value, (list, tuple))):
                    raise ValueError('The %s setting must be a list or a tuple. Please fix your settings.' % setting)
                setattr(self, setting, setting_value)
                self._explicit_settings.add(setting)

    def is_overridden(self, setting):
        return setting in self._explicit_settings

    def __repr__(self):
        return '<%(cls)s "%(settings_module)s">' % {
            'cls': self.__class__.__name__,
            'settings_module': self.SETTINGS_MODULE
        }

settings = Settings()


class BaseConf(object):

    # Name of the App
    APPNAME = settings.APPNAME
    # Getting the signal names
    SIGNAL_NAMES = dict((getattr(signal, n), n) for n in dir(signal) if n.startswith('SIG') and '_' not in n)
    # Translators: Cluster status descriptions
    STARTING = 'Starting'
    WORKING = 'Working'
    IDLE = 'Idle'
    STOPPED = 'Stopped'
    STOPPING = 'Stopping'


# logger
LOGGING_CONFIG = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logging.ini')
if os.path.isfile(LOGGING_CONFIG):
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
else:
    print 'WARNING: not logging config file exists!!!'
