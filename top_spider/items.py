# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

from top_spider.settings import mysql_table


class TopSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # 股票代码
    stock_code = scrapy.Field()
    # 股票名称
    stock_name = scrapy.Field()
    # 公告日期
    trade_date = scrapy.Field()
    # 开始日期
    start_date = scrapy.Field()
    # 上榜理由
    reason = scrapy.Field()
    # 上榜理由标签
    reason_label = scrapy.Field()
    # 成 交 量
    vol_amt = scrapy.Field()
    # 成交金额
    vol_value = scrapy.Field()

    # 买方
    buy_tag_1 = scrapy.Field()
    # 买一营业部名称
    buy_department_1 = scrapy.Field()
    # 买一买入金额
    buy_b_value_1 = scrapy.Field()
    # 买一卖出金额
    buy_s_value_1 = scrapy.Field()
    # 买一用户名称
    buy_user_name_1 = scrapy.Field()
    # 买一用户标签
    buy_user_label_1 = scrapy.Field()
    # 净额
    buy_net_1 = scrapy.Field()

    # 买方
    buy_tag_2 = scrapy.Field()
    # 买二营业部名称
    buy_department_2 = scrapy.Field()
    # 买二买入金额
    buy_b_value_2 = scrapy.Field()
    # 买二卖出金额
    buy_s_value_2 = scrapy.Field()
    # 买二用户名称
    buy_user_name_2 = scrapy.Field()
    # 买二用户标签
    buy_user_label_2 = scrapy.Field()
    # 净额
    buy_net_2 = scrapy.Field()

    # 买方
    buy_tag_3 = scrapy.Field()
    # 买三营业部名称
    buy_department_3 = scrapy.Field()
    # 买三买入金额
    buy_b_value_3 = scrapy.Field()
    # 买三卖出金额
    buy_s_value_3 = scrapy.Field()
    # 买三用户名称
    buy_user_name_3 = scrapy.Field()
    # 买三用户标签
    buy_user_label_3 = scrapy.Field()
    # 净额
    buy_net_3 = scrapy.Field()

    # 买方
    buy_tag_4 = scrapy.Field()
    # 买四营业部名称
    buy_department_4 = scrapy.Field()
    # 买四买入金额
    buy_b_value_4 = scrapy.Field()
    # 买四卖出金额
    buy_s_value_4 = scrapy.Field()
    # 买四用户名称
    buy_user_name_4 = scrapy.Field()
    # 买四用户标签
    buy_user_label_4 = scrapy.Field()
    # 净额
    buy_net_4 = scrapy.Field()

    # 买方
    buy_tag_5 = scrapy.Field()
    # 买五营业部名称
    buy_department_5 = scrapy.Field()
    # 买五买入金额
    buy_b_value_5 = scrapy.Field()
    # 买五卖出金额
    buy_s_value_5 = scrapy.Field()
    # 买五用户名称
    buy_user_name_5 = scrapy.Field()
    # 买五用户标签
    buy_user_label_5 = scrapy.Field()
    # 净额
    buy_net_5 = scrapy.Field()

    # 卖
    sell_tag_1 = scrapy.Field()
    # 卖一营业部名称
    sell_department_1 = scrapy.Field()
    # 卖一买入金额
    sell_b_value_1 = scrapy.Field()
    # 卖一卖出金额
    sell_s_value_1 = scrapy.Field()
    # 卖一用户名称
    sell_user_name_1 = scrapy.Field()
    # 卖一用户标签
    sell_user_label_1 = scrapy.Field()
    # 净额
    sell_net_1 = scrapy.Field()

    # 卖
    sell_tag_2 = scrapy.Field()
    # 卖二营业部名称
    sell_department_2 = scrapy.Field()
    # 卖二买入金额
    sell_b_value_2 = scrapy.Field()
    # 卖二卖出金额
    sell_s_value_2 = scrapy.Field()
    # 卖二用户名称
    sell_user_name_2 = scrapy.Field()
    # 卖二用户标签
    sell_user_label_2 = scrapy.Field()
    # 净额
    sell_net_2 = scrapy.Field()

    # 卖
    sell_tag_3 = scrapy.Field()
    # 卖三营业部名称
    sell_department_3 = scrapy.Field()
    # 卖三买入金额
    sell_b_value_3 = scrapy.Field()
    # 卖三卖出金额
    sell_s_value_3 = scrapy.Field()
    # 卖三用户名称
    sell_user_name_3 = scrapy.Field()
    # 卖三用户标签
    sell_user_label_3 = scrapy.Field()
    # 净额
    sell_net_3 = scrapy.Field()

    # 卖
    sell_tag_4 = scrapy.Field()
    # 卖四营业部名称
    sell_department_4 = scrapy.Field()
    # 卖四买入金额
    sell_b_value_4 = scrapy.Field()
    # 卖四卖出金额
    sell_s_value_4 = scrapy.Field()
    # 卖四用户名称
    sell_user_name_4 = scrapy.Field()
    # 卖四用户标签
    sell_user_label_4 = scrapy.Field()
    # 净额
    sell_net_4 = scrapy.Field()


    # 卖
    sell_tag_5 = scrapy.Field()
    # 卖五营业部名称
    sell_department_5 = scrapy.Field()
    # 卖五买入金额
    sell_b_value_5 = scrapy.Field()
    # 卖五卖出金额
    sell_s_value_5 = scrapy.Field()
    # 卖五用户名称
    sell_user_name_5 = scrapy.Field()
    # 卖五用户标签
    sell_user_label_5 = scrapy.Field()
    # 净额
    sell_net_5 = scrapy.Field()

    def get_insert_sql(self):
        # 插入：sql语句
        insert_sql = """insert into {0} (stock_code,stock_name ,trade_date,start_date,reason ,reason_label ,vol_amt ,vol_value ,
            buy_department_1 ,
            buy_b_value_1 ,
            buy_s_value_1 ,
            buy_user_name_1 ,
            buy_user_label_1 ,
            buy_net_1 ,
            buy_department_2,
            buy_b_value_2 ,
            buy_s_value_2 ,
            buy_user_name_2 ,
            buy_user_label_2 ,
            buy_net_2 ,
            buy_department_3,
            buy_b_value_3 ,
            buy_s_value_3 ,
            buy_user_name_3 ,
            buy_user_label_3 ,
            buy_net_3 ,
            buy_department_4,
            buy_b_value_4 ,
            buy_s_value_4 ,
            buy_user_name_4 ,
            buy_user_label_4 ,
            buy_net_4 ,
            buy_department_5,
            buy_b_value_5 ,
            buy_s_value_5 ,
            buy_user_name_5 ,
            buy_user_label_5 ,
            buy_net_5 ,
            sell_department_1,
            sell_b_value_1 ,
            sell_s_value_1 ,
            sell_user_name_1 ,
            sell_user_label_1 ,
            sell_net_1 ,
            sell_department_2,
            sell_b_value_2 ,
            sell_s_value_2 ,
            sell_user_name_2 ,
            sell_user_label_2 ,
            sell_net_2 ,
            sell_department_3,
            sell_b_value_3 ,
            sell_s_value_3 ,
            sell_user_name_3 ,
            sell_user_label_3 ,
            sell_net_3 ,
            sell_department_4,
            sell_b_value_4 ,
            sell_s_value_4 ,
            sell_user_name_4 ,
            sell_user_label_4 ,
            sell_net_4 ,
            sell_department_5,
            sell_b_value_5 ,
            sell_s_value_5 ,
            sell_user_name_5 ,
            sell_user_label_5 ,
            sell_net_5) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            ,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(mysql_table)

        params = (
            self['stock_code'],
            self['stock_name'],
            self['trade_date'],
            self['start_date'],
            self['reason'],
            self['reason_label'],
            self['vol_amt'],
            self['vol_value'],

            self['buy_department_1'],
            self['buy_b_value_1'],
            self['buy_s_value_1'],
            self['buy_user_name_1'],
            self['buy_user_label_1'],
            self['buy_net_1'],

            self['buy_department_2'],
            self['buy_b_value_2'],
            self['buy_s_value_2'],
            self['buy_user_name_2'],
            self['buy_user_label_2'],
            self['buy_net_2'],

            self['buy_department_3'],
            self['buy_b_value_3'],
            self['buy_s_value_3'],
            self['buy_user_name_3'],
            self['buy_user_label_3'],
            self['buy_net_3'],

            self['buy_department_4'],
            self['buy_b_value_4'],
            self['buy_s_value_4'],
            self['buy_user_name_4'],
            self['buy_user_label_4'],
            self['buy_net_4'],

            self['buy_department_5'],
            self['buy_b_value_5'],
            self['buy_s_value_5'],
            self['buy_user_name_5'],
            self['buy_user_label_5'],
            self['buy_net_5'],

            self['sell_department_1'],
            self['sell_b_value_1'],
            self['sell_s_value_1'],
            self['sell_user_name_1'],
            self['sell_user_label_1'],
            self['sell_net_1'],

            self['sell_department_2'],
            self['sell_b_value_2'],
            self['sell_s_value_2'],
            self['sell_user_name_2'],
            self['sell_user_label_2'],
            self['sell_net_2'],

            self['sell_department_3'],
            self['sell_b_value_3'],
            self['sell_s_value_3'],
            self['sell_user_name_3'],
            self['sell_user_label_3'],
            self['sell_net_3'],

            self['sell_department_4'],
            self['sell_b_value_4'],
            self['sell_s_value_4'],
            self['sell_user_name_4'],
            self['sell_user_label_4'],
            self['sell_net_4'],

            self['sell_department_5'],
            self['sell_b_value_5'],
            self['sell_s_value_5'],
            self['sell_user_name_5'],
            self['sell_user_label_5'],
            self['sell_net_5'],

        )
        return insert_sql, params

