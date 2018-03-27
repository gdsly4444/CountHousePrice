#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import urllib2
import socket
import time
import threading
class Distribute(object):
    def __init__(self,NumThread,func):    #线程数，任务集合，,线程函数
        self.NumThread=NumThread
        self.func=func
    #执行
    def execute(self):
        #多线程
        threads=[]
        for i in range(self.NumThread):
            t = threading.Thread(target=self.func)
            threads.append(t)
        start = time.time()
        for i in range(self.NumThread):
            threads[i].start()
        for i in range(self.NumThread):
            threads[i].join()
        end = time.time()
        return end-start