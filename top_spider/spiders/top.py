# -*- coding: utf-8 -*-
import re
import time
import json
import scrapy
import datetime
import random
import logging
from abc import ABC

from top_spider.items import TopSpiderItem
from top_spider.settings import Top_Level
from top_spider.utils import top_label
from top_spider.utils import data_structure, tools


class TopSpider(scrapy.Spider, ABC):
    name = 'top'
    allowed_domains = ['www.sse.com.cn', 'www.szse.cn']
    start_urls = ['http://www.sse.com.cn/', 'http://www.szse.cn/']

    dt = datetime.date.today()
    today = '2020-11-05'#dt.strftime('%Y-%m-%d')
    trade_date = today.replace('-', '')

    query = top_label.TopLabel()

    random_data = random.random()
    base_url = "http://www.szse.cn/api/report{0}"
    url = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1842_xxpl&TABKEY=tab1&PAGENO={0}&txtStart={1}&txtEnd={1}&random={2}"

    def start_requests(self):
        # 深圳
        page_no = 1
        yield scrapy.Request(
            self.url.format(page_no, self.today, self.random_data),
            meta={"pageNo": page_no},
            callback=self.parse_sz,
        )
        # 上海
        unix_time = int(round(time.time() * 1000))
        url = "http://query.sse.com.cn//marketdata/tradedata/queryAllTradeOpenDate.do?jsonCallBack=jsonpCallback37975&token=QUERY&tradeDate={0}&_={1}".format(
            self.trade_date, unix_time)
        url_kcb = "http://query.sse.com.cn/marketdata/tradedata/queryKCBTradeInfo.do?jsonCallBack=jsonpCallback94603&tradeDate={0}&_={1}".format(
            self.trade_date, unix_time)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
            "Host": "query.sse.com.cn",
            "Referer": "http://www.sse.com.cn/disclosure/diclosure/public/dailydata/"
        }
        yield scrapy.Request(url, headers=headers, callback=self.parse_sh)

        yield scrapy.Request(url_kcb, headers=headers, callback=self.parse_kcb)

    def parse_sz(self, response):
        page_no = response.meta.get("pageNo")
        logging.info("正在抓取第{0}页".format(page_no))

        result = response.text
        result = json.loads(result)
        result = result[0]["data"]
        if len(result) == 0:
            logging.info("日期为：{0} 网站没有数据".format(self.trade_date, page_no))
            return
        for info in result:
            announce_time = info["dqrq"]
            stock_code = info["zqdm"]
            stock_name = info["zqjc"]
            reason = info["plyy"]
            bz = info["bz"]

            item = TopSpiderItem()

            # 公司代码
            item["stock_code"] = stock_code + '.SZ'
            # 公司简称
            item["stock_name"] = stock_name.replace(';', '').replace('&', '').replace('nbsp', '')
            # 公告日期
            item["trade_date"] = announce_time
            # 理由
            item["reason"] = reason
            item['reason_label'] = ''

            link = re.findall("a-param='(.*?)'>", bz)[0]
            detail_url = self.base_url.format(link)
            yield scrapy.Request(
                detail_url, meta={"item": item}, callback=self.detail_content_parse
            )

        cur_page = page_no + 1
        logging.info("正在抓取深圳龙虎榜第：{0}页。".format(cur_page))
        yield scrapy.Request(
            self.url.format(cur_page, self.today, self.random_data),
            meta={"pageNo": cur_page},
            callback=self.parse_sz,
        )

    def parse_sh(self, response):
        result = response.text.split("(", 1)[1][:-1]
        json_str = json.loads(result)

        flag = json_str['pageHelp']
        if flag is None:
            logging.info('{0}号 暂无数据!'.format(self.trade_date))
            return

        json_str_list = json_str['pageHelp']['data']
        for info in json_str_list:
            info.setdefault('branchNameS')
            info.setdefault('branchTxAmtS')
            ref_type = info['refType']
            ref_type_value = data_structure.category_dict[ref_type]
            # 索引
            title_index = data_structure.titles[ref_type_value]
            # 按照索引取值
            reason = data_structure.reasons[title_index]

            trade_date = tools.date_convert(info['tradeDate'])
            abnormal_start = tools.date_convert(info['abnormalStart'])
            abnormal_end = tools.date_convert(info['abnormalEnd'])

            branch = info['branchNameS']
            item = TopSpiderItem()

            # 公司代码
            item["stock_code"] = info['secCode'] + ".SH"
            # 公司简称
            item["stock_name"] = info['secAbbr']
            # 公告日期
            item["trade_date"] = trade_date
            item["start_date"] = abnormal_start
            # 理由
            item["reason"] = reason
            item['reason_label'] = ''
            # 成 交 量
            item['vol_amt'] = info['secTxVolume']
            # 成交金额
            item['vol_value'] = info['secTxAmount']

            if branch:
                branch_name_b_list = tools.fill_list(info['branchNameB'].split(','))
                branch_tx_amt_b_list = tools.fill_list(info['branchTxAmtB'].split(','))

                branch_name_s_list = tools.fill_list(info['branchNameS'].split(','))
                branch_tx_amt_s_list = tools.fill_list(info['branchTxAmtS'].split(','))
            else:
                branch_name_b_list = ['-', '-', '-', '-', '-']
                branch_tx_amt_b_list = ['-', '-', '-', '-', '-']

                branch_name_s_list = ['-', '-', '-', '-', '-']
                branch_tx_amt_s_list = ['-', '-', '-', '-', '-']

            buy_b_value_1 = float(tools.is_none_value(branch_tx_amt_b_list[0]))
            buy_s_value_1 = 0.0
            buy_department_1 = tools.is_none_value(branch_name_b_list[0])
            # 买1
            item["buy_tag_1"] = "买1"
            # 会员营业部名称1
            item["buy_department_1"] = buy_department_1
            # 买入金额（元）
            item["buy_b_value_1"] = buy_b_value_1
            # 卖出金额（元）
            item["buy_s_value_1"] = buy_s_value_1
            # 买一用户名称
            item['buy_user_name_1'] = self.operation_user_info(buy_department_1, 'name')
            # 买一用户标签
            item['buy_user_label_1'] = self.operation_user_info(buy_department_1, 'label')
            # 净额
            item['buy_net_1'] = buy_b_value_1 - buy_s_value_1
            # elif org["mmlb"] == "买2":
            # 买2
            buy_b_value_2 = float(tools.is_none_value(branch_tx_amt_b_list[1]))
            buy_s_value_2 = 0.0
            buy_department_2 = tools.is_none_value(branch_name_b_list[1])
            item["buy_tag_2"] = "买2"
            item["buy_department_2"] = buy_department_2
            item["buy_b_value_2"] = buy_b_value_2
            item["buy_s_value_2"] = buy_s_value_2
            # 用户名称
            item['buy_user_name_2'] = self.operation_user_info(buy_department_2, 'name')
            # 用户标签
            item['buy_user_label_2'] = self.operation_user_info(buy_department_2, 'label')
            # 净额
            item['buy_net_2'] = buy_b_value_2 - buy_s_value_2
            # elif org["mmlb"] == "买3":
            # 买3
            buy_b_value_3 = float(tools.is_none_value(branch_tx_amt_b_list[2]))
            buy_s_value_3 = 0.0
            buy_department_3 = tools.is_none_value(branch_name_b_list[2])
            item["buy_tag_3"] = "买3"
            item["buy_department_3"] = buy_department_3
            item["buy_b_value_3"] = buy_b_value_3
            item["buy_s_value_3"] = buy_s_value_3
            # 用户名称
            item['buy_user_name_3'] = self.operation_user_info(buy_department_3, 'name')
            # 用户标签
            item['buy_user_label_3'] = self.operation_user_info(buy_department_3, 'label')
            # 净额
            item['buy_net_3'] = buy_b_value_3 - buy_s_value_3
            # elif org["mmlb"] == "买4":
            # 买4
            buy_b_value_4 = float(tools.is_none_value(branch_tx_amt_b_list[3]))
            buy_s_value_4 = 0
            buy_department_4 = tools.is_none_value(branch_name_b_list[3])
            item["buy_tag_4"] = "买4"
            item["buy_department_4"] = buy_department_4
            item["buy_b_value_4"] = buy_b_value_4
            item["buy_s_value_4"] = buy_s_value_4
            # 用户名称
            item['buy_user_name_4'] = self.operation_user_info(buy_department_4, 'name')
            # 用户标签
            item['buy_user_label_4'] = self.operation_user_info(buy_department_4, 'label')
            # 净额
            item['buy_net_4'] = buy_b_value_4 - buy_s_value_4
            # elif org["mmlb"] == "买5":
            # 买5
            buy_b_value_5 = float(tools.is_none_value(branch_tx_amt_b_list[4]))
            buy_s_value_5 = 0
            buy_department_5 = tools.is_none_value(branch_name_b_list[4])
            item["buy_tag_5"] = "买5"
            item["buy_department_5"] = buy_department_5
            item["buy_b_value_5"] = buy_b_value_5
            item["buy_s_value_5"] = buy_s_value_5
            # 用户名称
            item['buy_user_name_5'] = self.operation_user_info(buy_department_5, 'name')
            # 用户标签
            item['buy_user_label_5'] = self.operation_user_info(buy_department_5, 'label')
            # 净额
            item['buy_net_5'] = buy_b_value_5 - buy_s_value_5
            # elif org["mmlb"] == "卖1":
            # 卖1
            sell_b_value_1 = 0
            sell_s_value_1 = float(tools.is_none_value(branch_tx_amt_s_list[0]))
            sell_department_1 = tools.is_none_value(branch_name_s_list[0])
            item["sell_tag_1"] = "卖1"
            item["sell_department_1"] = sell_department_1
            item["sell_b_value_1"] = sell_b_value_1
            item["sell_s_value_1"] = sell_s_value_1
            # 卖一用户名称
            item['sell_user_name_1'] = self.operation_user_info(sell_department_1, 'name')
            # 卖一用户标签
            item['sell_user_label_1'] = self.operation_user_info(sell_department_1, 'label')
            # 净额
            item['sell_net_1'] = sell_b_value_1 - sell_s_value_1
            # elif org["mmlb"] == "卖2":
            # 卖2
            sell_b_value_2 = 0
            sell_s_value_2 = float(tools.is_none_value(branch_tx_amt_s_list[1]))
            sell_department_2 = tools.is_none_value(branch_name_s_list[1])
            item["sell_tag_2"] = "卖2"
            item["sell_department_2"] = sell_department_2
            item["sell_b_value_2"] = sell_b_value_2
            item["sell_s_value_2"] = sell_s_value_2
            # 卖一用户名称
            item['sell_user_name_2'] = self.operation_user_info(sell_department_2, 'name')
            # 卖一用户标签
            item['sell_user_label_2'] = self.operation_user_info(sell_department_2, 'label')
            # 净额
            item['sell_net_2'] = sell_b_value_2 - sell_s_value_2
            # elif org["mmlb"] == "卖3":
            # 卖3
            sell_b_value_3 = 0
            sell_s_value_3 = float(tools.is_none_value(branch_tx_amt_s_list[2]))
            sell_department_3 = tools.is_none_value(branch_name_s_list[2])
            item["sell_tag_3"] = "卖3"
            item["sell_department_3"] = sell_department_3
            item["sell_b_value_3"] = sell_b_value_3
            item["sell_s_value_3"] = sell_s_value_3
            # 卖三用户名称
            item['sell_user_name_3'] = self.operation_user_info(sell_department_3, 'name')
            # 卖三用户标签
            item['sell_user_label_3'] = self.operation_user_info(sell_department_3, 'label')
            # 净额
            item['sell_net_3'] = sell_b_value_3 - sell_s_value_3
            # elif org["mmlb"] == "卖4":
            #     # 卖4
            sell_b_value_4 = 0
            sell_s_value_4 = float(tools.is_none_value(branch_tx_amt_s_list[3]))
            sell_department_4 = tools.is_none_value(branch_name_s_list[3])
            item["sell_tag_4"] = "卖4"
            item["sell_department_4"] = sell_department_4
            item["sell_b_value_4"] = sell_b_value_4
            item["sell_s_value_4"] = sell_s_value_4
            # 卖四用户名称
            item['sell_user_name_4'] = self.operation_user_info(sell_department_4, 'name')
            # 卖四用户标签
            item['sell_user_label_4'] = self.operation_user_info(sell_department_4, 'label')
            # 净额
            item['sell_net_4'] = sell_b_value_4 - sell_s_value_4
            # elif org["mmlb"] == "卖5":
            # 卖5
            sell_b_value_5 = 0
            sell_s_value_5 = float(tools.is_none_value(branch_tx_amt_s_list[4]))
            sell_department_5 = tools.is_none_value(branch_name_s_list[4])
            item["sell_tag_5"] = "卖5"
            item["sell_department_5"] = sell_department_5
            item["sell_b_value_5"] = sell_b_value_5
            item["sell_s_value_5"] = sell_s_value_5
            # 卖五用户名称
            item['sell_user_name_5'] = self.operation_user_info(sell_department_5, 'name')
            # 卖五用户标签
            item['sell_user_label_5'] = self.operation_user_info(sell_department_5, 'label')
            # 净额
            item['sell_net_5'] = sell_b_value_5 - sell_s_value_5
            yield item

    def parse_kcb(self, response):
        text_kcb = response.text.split("(", 1)[1][:-1]
        json_str_kcb = json.loads(text_kcb)

        flag_kcb = json_str_kcb['pageHelp']['data']
        if len(flag_kcb) == 0:
            logging.info('{0}号 KCB 暂无数据!'.format(self.trade_date))
            return

        json_kcb_list = json_str_kcb['pageHelp']['data']
        for kcb in json_kcb_list:
            ref_type = kcb['refType']
            ref_type_value = data_structure.category_dict_kcb[ref_type]
            # 索引
            title_index = data_structure.titles_kcb[ref_type_value]
            # 按照索引取值
            reason = data_structure.reasons_kcb[title_index]

            dept_name_b = kcb['deptNameB']
            trade_date = tools.date_convert(kcb['tradeDate'])
            abnormal_start = tools.date_convert(kcb['unnormalStartDate'])
            abnormal_end = tools.date_convert(kcb['unnormalEndDate'])

            item = TopSpiderItem()

            # 公司代码
            item["stock_code"] = kcb['secCode'] + ".SH"
            # 公司简称
            item["stock_name"] = kcb['secAbbr']
            # 公告日期
            item["trade_date"] = trade_date
            item["start_date"] = abnormal_start
            # 理由
            item["reason"] = reason
            item['reason_label'] = ''
            # 成 交 量
            item['vol_amt'] = tools.value_convert_kcb(kcb['regionTxVol'])
            # 成交金额
            item['vol_value'] = tools.value_convert_kcb(kcb['regionTxAmt'])

            if dept_name_b:
                branch_name_b_list = tools.fill_list(kcb['deptNameB'].split(','))
                branch_tx_amt_b_list = tools.fill_list(kcb['deptTxAmtB'].split(','))

                branch_name_s_list = tools.fill_list(kcb['deptNameS'].split(','))
                branch_tx_amt_s_list = tools.fill_list(kcb['deptTxAmtS'].split(','))
            else:
                branch_name_b_list = ['-', '-', '-', '-', '-']
                branch_tx_amt_b_list = ['-', '-', '-', '-', '-']

                branch_name_s_list = ['-', '-', '-', '-', '-']
                branch_tx_amt_s_list = ['-', '-', '-', '-', '-']

            buy_b_value_1 = float(tools.value_convert_kcb(branch_tx_amt_b_list[0]))
            buy_s_value_1 = 0.0
            buy_department_1 = tools.is_none_value(branch_name_b_list[0])
            # 买1
            item["buy_tag_1"] = "买1"
            # 会员营业部名称1
            item["buy_department_1"] = buy_department_1
            # 买入金额（元）
            item["buy_b_value_1"] = buy_b_value_1
            # 卖出金额（元）
            item["buy_s_value_1"] = buy_s_value_1
            # 买一用户名称
            item['buy_user_name_1'] = self.operation_user_info(buy_department_1, 'name')
            # 买一用户标签
            item['buy_user_label_1'] = self.operation_user_info(buy_department_1, 'label')
            # 净额
            item['buy_net_1'] = buy_b_value_1 - buy_s_value_1
            # elif org["mmlb"] == "买2":
            # 买2
            buy_b_value_2 = float(tools.value_convert_kcb(branch_tx_amt_b_list[1]))
            buy_s_value_2 = 0.0
            buy_department_2 = tools.is_none_value(branch_name_b_list[1])
            item["buy_tag_2"] = "买2"
            item["buy_department_2"] = buy_department_2
            item["buy_b_value_2"] = buy_b_value_2
            item["buy_s_value_2"] = buy_s_value_2
            # 用户名称
            item['buy_user_name_2'] = self.operation_user_info(buy_department_2, 'name')
            # 用户标签
            item['buy_user_label_2'] = self.operation_user_info(buy_department_2, 'label')
            # 净额
            item['buy_net_2'] = buy_b_value_2 - buy_s_value_2
            # elif org["mmlb"] == "买3":
            # 买3
            buy_b_value_3 = float(tools.value_convert_kcb(branch_tx_amt_b_list[2]))
            buy_s_value_3 = 0.0
            buy_department_3 = tools.is_none_value(branch_name_b_list[2])
            item["buy_tag_3"] = "买3"
            item["buy_department_3"] = buy_department_3
            item["buy_b_value_3"] = buy_b_value_3
            item["buy_s_value_3"] = buy_s_value_3
            # 用户名称
            item['buy_user_name_3'] = self.operation_user_info(buy_department_3, 'name')
            # 用户标签
            item['buy_user_label_3'] = self.operation_user_info(buy_department_3, 'label')
            # 净额
            item['buy_net_3'] = buy_b_value_3 - buy_s_value_3
            # elif org["mmlb"] == "买4":
            # 买4
            buy_b_value_4 = float(tools.value_convert_kcb(branch_tx_amt_b_list[3]))
            buy_s_value_4 = 0
            buy_department_4 = tools.is_none_value(branch_name_b_list[3])
            item["buy_tag_4"] = "买4"
            item["buy_department_4"] = buy_department_4
            item["buy_b_value_4"] = buy_b_value_4
            item["buy_s_value_4"] = buy_s_value_4
            # 用户名称
            item['buy_user_name_4'] = self.operation_user_info(buy_department_4, 'name')
            # 用户标签
            item['buy_user_label_4'] = self.operation_user_info(buy_department_4, 'label')
            # 净额
            item['buy_net_4'] = buy_b_value_4 - buy_s_value_4
            # elif org["mmlb"] == "买5":
            # 买5
            buy_b_value_5 = float(tools.value_convert_kcb(branch_tx_amt_b_list[4]))
            buy_s_value_5 = 0
            buy_department_5 = tools.is_none_value(branch_name_b_list[4])
            item["buy_tag_5"] = "买5"
            item["buy_department_5"] = buy_department_5
            item["buy_b_value_5"] = buy_b_value_5
            item["buy_s_value_5"] = buy_s_value_5
            # 用户名称
            item['buy_user_name_5'] = self.operation_user_info(buy_department_5, 'name')
            # 用户标签
            item['buy_user_label_5'] = self.operation_user_info(buy_department_5, 'label')
            # 净额
            item['buy_net_5'] = buy_b_value_5 - buy_s_value_5
            # elif org["mmlb"] == "卖1":
            # 卖1
            sell_b_value_1 = 0
            sell_s_value_1 = float(tools.value_convert_kcb(branch_tx_amt_s_list[0]))
            sell_department_1 = tools.is_none_value(branch_name_s_list[0])
            item["sell_tag_1"] = "卖1"
            item["sell_department_1"] = sell_department_1
            item["sell_b_value_1"] = sell_b_value_1
            item["sell_s_value_1"] = sell_s_value_1
            # 卖一用户名称
            item['sell_user_name_1'] = self.operation_user_info(sell_department_1, 'name')
            # 卖一用户标签
            item['sell_user_label_1'] = self.operation_user_info(sell_department_1, 'label')
            # 净额
            item['sell_net_1'] = sell_b_value_1 - sell_s_value_1
            # elif org["mmlb"] == "卖2":
            # 卖2
            sell_b_value_2 = 0
            sell_s_value_2 = float(tools.value_convert_kcb(branch_tx_amt_s_list[1]))
            sell_department_2 = tools.is_none_value(branch_name_s_list[1])
            item["sell_tag_2"] = "卖2"
            item["sell_department_2"] = sell_department_2
            item["sell_b_value_2"] = sell_b_value_2
            item["sell_s_value_2"] = sell_s_value_2
            # 卖一用户名称
            item['sell_user_name_2'] = self.operation_user_info(sell_department_2, 'name')
            # 卖一用户标签
            item['sell_user_label_2'] = self.operation_user_info(sell_department_2, 'label')
            # 净额
            item['sell_net_2'] = sell_b_value_2 - sell_s_value_2
            # elif org["mmlb"] == "卖3":
            # 卖3
            sell_b_value_3 = 0
            sell_s_value_3 = float(tools.value_convert_kcb(branch_tx_amt_s_list[2]))
            sell_department_3 = tools.is_none_value(branch_name_s_list[2])
            item["sell_tag_3"] = "卖3"
            item["sell_department_3"] = sell_department_3
            item["sell_b_value_3"] = sell_b_value_3
            item["sell_s_value_3"] = sell_s_value_3
            # 卖三用户名称
            item['sell_user_name_3'] = self.operation_user_info(sell_department_3, 'name')
            # 卖三用户标签
            item['sell_user_label_3'] = self.operation_user_info(sell_department_3, 'label')
            # 净额
            item['sell_net_3'] = sell_b_value_3 - sell_s_value_3
            # elif org["mmlb"] == "卖4":
            #     # 卖4
            sell_b_value_4 = 0
            sell_s_value_4 = float(tools.value_convert_kcb(branch_tx_amt_s_list[3]))
            sell_department_4 = tools.is_none_value(branch_name_s_list[3])
            item["sell_tag_4"] = "卖4"
            item["sell_department_4"] = sell_department_4
            item["sell_b_value_4"] = sell_b_value_4
            item["sell_s_value_4"] = sell_s_value_4
            # 卖四用户名称
            item['sell_user_name_4'] = self.operation_user_info(sell_department_4, 'name')
            # 卖四用户标签
            item['sell_user_label_4'] = self.operation_user_info(sell_department_4, 'label')
            # 净额
            item['sell_net_4'] = sell_b_value_4 - sell_s_value_4
            # elif org["mmlb"] == "卖5":
            # 卖5
            sell_b_value_5 = 0
            sell_s_value_5 = float(tools.value_convert_kcb(branch_tx_amt_s_list[4]))
            sell_department_5 = tools.is_none_value(branch_name_s_list[4])
            item["sell_tag_5"] = "卖5"
            item["sell_department_5"] = sell_department_5
            item["sell_b_value_5"] = sell_b_value_5
            item["sell_s_value_5"] = sell_s_value_5
            # 卖五用户名称
            item['sell_user_name_5'] = self.operation_user_info(sell_department_5, 'name')
            # 卖五用户标签
            item['sell_user_label_5'] = self.operation_user_info(sell_department_5, 'label')
            # 净额
            item['sell_net_5'] = sell_b_value_5 - sell_s_value_5
            yield item

    def detail_content_parse(self, response):
            item = response.meta.get("item")

            result = response.text
            result = json.loads(result)
            # 公司基本信息
            base_info = result[0]["data"][0]
            # 会员营业部信息
            org_info = result[1]["data"]

            start_date = base_info["ycqj"].split("至")[0]
            if start_date == '无':
                start_date = None
            vol_amt = base_info["cjsl"].split(" ")[0].replace(',', '')
            vol_value = base_info["cjje"].split(" ")[0].replace(',', '')

            # 开始日期
            item["start_date"] = start_date
            # 成 交 量
            item["vol_amt"] = vol_amt
            # 成交金额
            item["vol_value"] = vol_value

            old_keys = ('买1', '买2', '买3', '买4', '买5', '卖1', '卖2', '卖3', '卖4', '卖5')
            new_keys = []
            new_results = []

            for info in org_info:
                key = info['mmlb']
                new_keys.append(key)
                new_results.append(info)

            for old_value in old_keys:
                if old_value not in new_keys:
                    set_default_value = {"mmlb": old_value, "zsmc": None, "mrje": "0", "mcje": "0"}
                    new_results.append(set_default_value)

            for org in new_results:
                if org["mmlb"] == "买1":
                    buy_b_value_1 = float(org["mrje"].replace(',', ''))
                    buy_s_value_1 = float(org["mcje"].replace(',', ''))
                    # 买1
                    item["buy_tag_1"] = org["mmlb"]
                    # 会员营业部名称1
                    item["buy_department_1"] = org["zsmc"]
                    # 买入金额（元）
                    item["buy_b_value_1"] = buy_b_value_1
                    # 卖出金额（元）
                    item["buy_s_value_1"] = buy_s_value_1
                    # 买一用户名称
                    item['buy_user_name_1'] = self.operation_user_info(org["zsmc"], 'name')
                    # 买一用户标签
                    item['buy_user_label_1'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['buy_net_1'] = buy_b_value_1 - buy_s_value_1
                elif org["mmlb"] == "买2":
                    # 买2
                    buy_b_value_2 = float(org["mrje"].replace(',', ''))
                    buy_s_value_2 = float(org["mcje"].replace(',', ''))
                    item["buy_tag_2"] = org["mmlb"]
                    item["buy_department_2"] = org["zsmc"]
                    item["buy_b_value_2"] = buy_b_value_2
                    item["buy_s_value_2"] = buy_s_value_2
                    # 用户名称
                    item['buy_user_name_2'] = self.operation_user_info(org["zsmc"], 'name')
                    # 用户标签
                    item['buy_user_label_2'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['buy_net_2'] = buy_b_value_2 - buy_s_value_2
                elif org["mmlb"] == "买3":
                    # 买3
                    buy_b_value_3 = float(org["mrje"].replace(',', ''))
                    buy_s_value_3 = float(org["mcje"].replace(',', ''))
                    item["buy_tag_3"] = org["mmlb"]
                    item["buy_department_3"] = org["zsmc"]
                    item["buy_b_value_3"] = buy_b_value_3
                    item["buy_s_value_3"] = buy_s_value_3
                    # 用户名称
                    item['buy_user_name_3'] = self.operation_user_info(org["zsmc"], 'name')
                    # 用户标签
                    item['buy_user_label_3'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['buy_net_3'] = buy_b_value_3 - buy_s_value_3
                elif org["mmlb"] == "买4":
                    # 买4
                    buy_b_value_4 = float(org["mrje"].replace(',', ''))
                    buy_s_value_4 = float(org["mcje"].replace(',', ''))
                    item["buy_tag_4"] = org["mmlb"]
                    item["buy_department_4"] = org["zsmc"]
                    item["buy_b_value_4"] = buy_b_value_4
                    item["buy_s_value_4"] = buy_s_value_4
                    # 用户名称
                    item['buy_user_name_4'] = self.operation_user_info(org["zsmc"], 'name')
                    # 用户标签
                    item['buy_user_label_4'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['buy_net_4'] = buy_b_value_4 - buy_s_value_4
                elif org["mmlb"] == "买5":
                    # 买5
                    buy_b_value_5 = float(org["mrje"].replace(',', ''))
                    buy_s_value_5 = float(org["mcje"].replace(',', ''))
                    item["buy_tag_5"] = org["mmlb"]
                    item["buy_department_5"] = org["zsmc"]
                    item["buy_b_value_5"] = buy_b_value_5
                    item["buy_s_value_5"] = buy_s_value_5
                    # 用户名称
                    item['buy_user_name_5'] = self.operation_user_info(org["zsmc"], 'name')
                    # 用户标签
                    item['buy_user_label_5'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['buy_net_5'] = buy_b_value_5 - buy_s_value_5
                elif org["mmlb"] == "卖1":
                    # 卖1
                    sell_b_value_1 = float(org["mrje"].replace(',', ''))
                    sell_s_value_1 = float(org["mcje"].replace(',', ''))
                    item["sell_tag_1"] = org["mmlb"]
                    item["sell_department_1"] = org["zsmc"]
                    item["sell_b_value_1"] = sell_b_value_1
                    item["sell_s_value_1"] = sell_s_value_1
                    # 卖一用户名称
                    item['sell_user_name_1'] = self.operation_user_info(org["zsmc"], 'name')
                    # 卖一用户标签
                    item['sell_user_label_1'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['sell_net_1'] = sell_b_value_1 - sell_s_value_1
                elif org["mmlb"] == "卖2":
                    # 卖2
                    sell_b_value_2 = float(org["mrje"].replace(',', ''))
                    sell_s_value_2 = float(org["mcje"].replace(',', ''))
                    item["sell_tag_2"] = org["mmlb"]
                    item["sell_department_2"] = org["zsmc"]
                    item["sell_b_value_2"] = sell_b_value_2
                    item["sell_s_value_2"] = sell_s_value_2
                    # 卖一用户名称
                    item['sell_user_name_2'] = self.operation_user_info(org["zsmc"], 'name')
                    # 卖一用户标签
                    item['sell_user_label_2'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['sell_net_2'] = sell_b_value_2 - sell_s_value_2
                elif org["mmlb"] == "卖3":
                    # 卖3
                    sell_b_value_3 = float(org["mrje"].replace(',', ''))
                    sell_s_value_3 = float(org["mcje"].replace(',', ''))
                    item["sell_tag_3"] = org["mmlb"]
                    item["sell_department_3"] = org["zsmc"]
                    item["sell_b_value_3"] = sell_b_value_3
                    item["sell_s_value_3"] = sell_s_value_3
                    # 卖三用户名称
                    item['sell_user_name_3'] = self.operation_user_info(org["zsmc"], 'name')
                    # 卖三用户标签
                    item['sell_user_label_3'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['sell_net_3'] = sell_b_value_3 - sell_s_value_3
                elif org["mmlb"] == "卖4":
                    # 卖4
                    sell_b_value_4 = float(org["mrje"].replace(',', ''))
                    sell_s_value_4 = float(org["mcje"].replace(',', ''))
                    item["sell_tag_4"] = org["mmlb"]
                    item["sell_department_4"] = org["zsmc"]
                    item["sell_b_value_4"] = sell_b_value_4
                    item["sell_s_value_4"] = sell_s_value_4
                    # 卖四用户名称
                    item['sell_user_name_4'] = self.operation_user_info(org["zsmc"], 'name')
                    # 卖四用户标签
                    item['sell_user_label_4'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['sell_net_4'] = sell_b_value_4 - sell_s_value_4
                elif org["mmlb"] == "卖5":
                    # 卖5
                    sell_b_value_5 = float(org["mrje"].replace(',', ''))
                    sell_s_value_5 = float(org["mcje"].replace(',', ''))
                    item["sell_tag_5"] = org["mmlb"]
                    item["sell_department_5"] = org["zsmc"]
                    item["sell_b_value_5"] = sell_b_value_5
                    item["sell_s_value_5"] = sell_s_value_5
                    # 卖五用户名称
                    item['sell_user_name_5'] = self.operation_user_info(org["zsmc"], 'name')
                    # 卖五用户标签
                    item['sell_user_label_5'] = self.operation_user_info(org["zsmc"], 'label')
                    # 净额
                    item['sell_net_5'] = sell_b_value_5 - sell_s_value_5
            yield item

    def operation_user_info(self, dep_name, tag):
        rows = self.query.query_user_label(dep_name)
        if rows:
            if tag == 'name':
                result = rows[0]
                return result
            elif tag == 'label':
                result = rows[1]
                return Top_Level[result]
