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
# append_tables = [i for i in map_dict.keys()]


# WIND_TABLES = [map_dict[i]['wind'] for i in map_dict]
# SUNTIME_TABLES = [map_dict[i]['suntime'] for i in map_dict]


from concurrent.futures import *  # 多进程并行


# 需要的csc表
def down_table_by_merge(csc_merge_table):
    # 1.根据映射关系获取待下载的所有数据源 map_dict[csc_merge_table].keys()
    # 2.获取每个数据源下的所有表
    all_tables = []
    for db in [i for i in map_dict[csc_merge_table].keys()]:
        all_tables += [
            (db + '_af_connector', table,
             ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr['target_column']))),
             attr['date_column'],) for table, attr in map_dict[csc_merge_table][db].items()]
    # 执行SQL的SELECT
    for i in all_tables:
        extract_sql(i[0], i[1], i[2], i[3], 20220101, 20220401)
        # print(i[2])
        # s = f'# SELECT {i[2]} FROM `{i[1]}` WHERE `ann_date`=22'
        # print(s)


def extract_sql(connector_id, table_name, column, date_column, start_date, end_date):
    query_sql = f'SELECT {column} FROM `{table_name}` WHERE `{date_column}` BETWEEN {start_date} AND {end_date} '
    print('\n', query_sql, connector_id)


# # down_table = [i for i in map_dict.keys()]
# with ThreadPoolExecutor(max_workers=2) as executor:  # 多进程异步执行
#     _ = {executor.submit(lambda x: down_table_by_merge(x),
#                          table): table for table in map_dict.keys()}

for i in map_dict.keys():
    down_table_by_merge(i)
