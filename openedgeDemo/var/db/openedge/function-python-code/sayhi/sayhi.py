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
    edgeTestCases.function handler
    """
    result = {}

    if 'exception' in event:
        result['exception'] = 1 / 0

    if 'value' in event: 
        result['value'] = event['value']

    if 'no_return_value' in event:
        return None

    if 'timeout' in event:
        while True:
            pass

    if 'USER_NAME' in os.environ:
        result['user_name'] = os.environ['USER_NAME']
   
    result['context'] = context
    result['chinese_msg'] = '你好'

    return result


def run(event):
    """
    edgeTestCases.function run thread
    """
    for i in range(1, 10):
        event['run.thread.times'] = i
        time.sleep(5)


if __name__ == '__main__':
    print handler({'id': 11}, {})
