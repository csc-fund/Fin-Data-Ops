#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :json_unit_test.py
# @Time      :2022/9/20 15:48
# @Author    :Colin
# @Note      :None

from map_table import MapCsc

# 注册
app = MapCsc('CSC_Profit_Express')

# SQL语句
sqls = app.get_map_tables()

for connector_id, table_name, column, date_column, code_column, column_len in sqls:
    query_sql = f'SELECT {column} FROM `{table_name}` WHERE `{date_column}` BETWEEN {20220101} AND {20221231} '
    print(query_sql)
    # print(connector_id, table_name, column, date_column, code_column, column_len)
