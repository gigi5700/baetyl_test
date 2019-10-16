#!/usr/bin/env python
#-*- coding:utf-8 -*-
"""
module to say hi
"""

import os
import time
import threading


def handler(event, context):
    """
    function handler
    """
    return event

if __name__ == '__main__':
    print handler({'id': 11}, {})
