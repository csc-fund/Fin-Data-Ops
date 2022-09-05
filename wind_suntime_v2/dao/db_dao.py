#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :db_dao.py
# @Time      :2022/9/5 08:46
# @Author    :Colin
# @Note      :None

from settings import *
import mysql.connector


class Dbutil:
    def __init__(self, data_source):
        # 按照数据源初始化数据库配置
        self.data_source = data_source
        self.db_host = DB_CONFIG[self.data_source]['host']
        self.db_port = DB_CONFIG[self.data_source]['port']
        self.db_user = DB_CONFIG[self.data_source]['user']
        self.db_pwd = DB_CONFIG[self.data_source]['pwd']
        # 建立数据库连接
        self.__get_conn()

    def __get_conn(self):
        self.db_conn = mysql.connector.connect(user=self.db_user, password=self.db_pwd,
                                               host=self.db_host, port=self.db_port,
                                               )

    def get_table(self, table_name, table_type='df'):
        # 获取游标
        self.db_cur = self.db_conn.cursor(buffered=True)
        if table_type == 'df':
            pass

    def __exit__(self, type, value, trace):
        self.db_conn.close()


app = Dbutil('test')
