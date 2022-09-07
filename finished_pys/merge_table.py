#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None
import pandas as pd
from table_map import map_dict

# 1.分别下载总表中的子表
#
append_tables = [i for i in map_dict.keys()]


# WIND_TABLES = [map_dict[i]['wind'] for i in map_dict]
# SUNTIME_TABLES = [map_dict[i]['suntime'] for i in map_dict]


# 需要的csc表
def down_by_csc(target_table):
    # 根据映射关系获取待下载的表名
    # 表2:待合并的表
    csc_table = target_table

    # 表1-1:要下载的表
    wind_tables = [
        ('wind_conn', table, ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr))), '20220202') for
        table, attr in map_dict[csc_table]['wind'].items()]
    for i in wind_tables:
        extract_sql(i[0], i[1], i[2], i[3])

    # 表1-2:要下载的表
    suntime_tables = [
        ('suntime_conn', table, ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr))), '20220202') for
        table, attr in map_dict[csc_table]['suntime'].items()]
    for i in suntime_tables:
        extract_sql(i[0], i[1], i[2], i[3])

    # 表1-n:要下载的表
    # ...

    # 一次下载2张表 原始表1,原始表2,多源合并3
    # print(csc_table, wind_tables, suntime_tables)
    # 获取要下载的字段


def extract_sql(connector, table_name, column, date, ):
    query_sql = 'SELECT {column} FROM `{table_name}` WHERE `ann_date`={ann_date}'.format(column=column,
                                                                                         table_name=table_name,
                                                                                         ann_date=date)

    print(query_sql, connector)


down_by_csc('CSC_Test')
