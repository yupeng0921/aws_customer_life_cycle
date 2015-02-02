#!/usr/bin/env python

import logging

log_level_mapping = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}

def get_log_level(log_level):
    return log_level_mapping.get(log_level)
