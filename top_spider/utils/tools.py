#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2020/11/26 16:03
# @Author  : Oscar
import datetime
from dateutil.parser import parse


def get_n_day_list(n):
    before_n_days = []
    for i in range(1, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days


def date_convert(trade_date):
    # 时间格式转换：20180926-->2018-09-26
    date = parse(trade_date)
    dt = date.strftime("%Y-%m-%d")
    return dt


def is_none_value(value):
    # 如果传入值为‘-’，则返回None
    if value == '-':
        return 0.0
    else:
        return value


def value_convert_kcb(num):
    if num == '-':
        return None
    else:
        number = round(float(num) * 10000, 2)
        return number


def fill_list(tag_list):
    # 列表长度小于5,则填充列表让元素长度满足5
    tag_list_len = len(tag_list)
    if tag_list_len < 5:
        add_len = 5 - tag_list_len
        for i in range(1, add_len + 1):
            tag_list.extend('--')
    return tag_list
