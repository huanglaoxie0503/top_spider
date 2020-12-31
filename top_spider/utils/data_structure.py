#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2020/11/26 16:05
# @Author  : Oscar

category_dict_kcb = {
    "1": "有价格涨跌幅限制的日收盘价格涨幅达到15%的前五只证券",
    "2": "有价格涨跌幅限制的日收盘价格跌幅达到15%的前五只证券",
    "3": "有价格涨跌幅限制的日价格振幅达到30%的前五只证券",
    "4": "有价格涨跌幅限制的日换手率达到30%的前五只证券",
    "5": "有价格涨跌幅限制的连续3个交易日内收盘价格涨幅偏离值累计达到30%的证券",
    "6": "有价格涨跌幅限制的连续3个交易日内收盘价格跌幅偏离值累计达到30%的证券",
    "7": "实施特别停牌的证券"
}

titles_kcb = {
    "有价格涨跌幅限制的日收盘价格涨幅达到15%的前五只证券": 1,
    "有价格涨跌幅限制的日收盘价格跌幅达到15%的前五只证券": 2,
    "有价格涨跌幅限制的日价格振幅达到30%的前五只证券": 3,
    "有价格涨跌幅限制的日换手率达到30%的前五只证券": 4,
    "有价格涨跌幅限制的连续3个交易日内收盘价格涨幅偏离值累计达到30%的证券": 5,
    "有价格涨跌幅限制的连续3个交易日内收盘价格跌幅偏离值累计达到30%的证券": 6,
    "实施特别停牌的证券": 7
}

reasons_kcb = (
    "",
    "日涨幅偏离值达到15%",
    "日跌幅偏离值达到15%",
    "日价格振幅达到30%",
    "日换手率达到30%",
    "3日涨幅偏离值达到30%",
    "3日跌幅偏离值达到30%",
    "实施特别停牌的证券"
)

category_dict = {
    "11": "有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前三只证券",
    "12": "有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前三只证券",
    "13": "有价格涨跌幅限制的日价格振幅达到15%的前三只证券",
    "14": "有价格涨跌幅限制的日换手率达到20%的前三只证券",
    "15": "无价格涨跌幅限制的证券",
    "1": "非ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券",
    "2": "非ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券",
    "00": "ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券",
    "4": "ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到15%的证券",
    "0": "连续三个交易日内的日均换手率与前五个交易日日均换手率的比值到达30倍,并且该股票封闭式基金连续三个交易日内累计换手率达到20%",
    "31": "当日无价格涨跌幅限制的A股，出现异常波动停牌的",
    "000": "当日价格涨跌幅限制的B股，出现异常波动停牌的",
    "41": "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上",
    "0000": "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上",
    "000000": "风险警示股票盘中换手率达到或超过30%",
    "51": "退市整理的证券",
    "00000000": "实施特别停牌的证券",
}

titles = dict()
titles["有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前三只证券"] = 0
titles["有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前三只证券"] = 1
titles["有价格涨跌幅限制的日价格振幅达到15%的前三只证券"] = 2
titles["有价格涨跌幅限制的日换手率达到20%的前三只证券"] = 3
titles["无价格涨跌幅限制的证券"] = 4
titles["非ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券"] = 5
titles["非ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券"] = 6
titles["ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券"] = 7
titles["ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到15%的证券"] = 8
titles["连续三个交易日内的日均换手率与前五个交易日日均换手率的比值到达30倍"] = 9
titles["当日无价格涨跌幅限制的A股，出现异常波动停牌的"] = 10
titles["当日无价格涨跌幅限制的B股，出现异常波动停牌的"] = 11
titles["单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上"] = 12
titles["单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上"] = 13
titles["风险警示股票盘中换手率达到或超过30%"] = 14
titles["退市整理的证券"] = 15
titles["实施特别停牌的证券"] = 16

reasons = [
    "日涨幅偏离值达到7%",
    "日跌幅偏离值达到7%",
    "日价格振幅达到15%",
    "日换手率达到20%",
    "无价格涨跌幅限制",
    "非ST三个交易日内价格涨幅累计达到20%",
    "非ST三个交易日内价格跌幅累计达到20%",
    "ST三个交易日内价格涨幅累计达到15%",
    "ST三个交易日内价格跌幅累计达到15%",
    "三个交易日内的日均换手率与前五个交易日日均换手率的比值到达30倍",
    "无价格涨跌幅限制的A股，出现异常波动停牌",
    "无价格涨跌幅限制的B股，出现异常波动停牌",
    "融资买入达到当日该证券总交易量的50％以上",
    "融券卖出达到当日该证券总交易量的50％以上",
    "风险警示股票盘中换手率达到或超过30%",
    "退市整理的证券",
    "实施特别停牌的证券",
]
