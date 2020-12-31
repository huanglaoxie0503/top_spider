#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2020/11/26 13:56
# @Author  : Oscar
import os
from scrapy import cmdline

if __name__ == '__main__':
    file_log = os.getcwd() + "/info.log"
    if os.path.exists(file_log):
        os.remove(file_log)
        print("每次运行前把之前的日志文件删除,保留最新日志即可")

    cmdline.execute("scrapy crawl top".split())
