#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2020/11/26 14:29
# @Author  : Oscar
import pymysql

from top_spider.settings import mysql_host, mysql_user, mysql_password, mysql_db, mysql_table_department, \
    mysql_table_user


class TopLabel(object):
    def __init__(self):
        self.conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db
        )
        self.cursor = self.conn.cursor()

    def query_user_label(self, department_name):
        try:
            sql = """
                SELECT b.user_name, b.user_level
                FROM top_department as a 
                JOIN top_user as b on a.user_id = b.user_id
                WHERE a.department_name=%s;
            """
            self.cursor.execute(sql, (department_name, ))
            results = self.cursor.fetchall()
            if results:
                return results[0]
        except pymysql.Error as e:
            print(e)


if __name__ == '__main__':
    order = TopLabel()
    name = '东方财富证券股份有限公司拉萨团结路第一证券营业部'
    info = order.query_user_label(name)
    print(info)
