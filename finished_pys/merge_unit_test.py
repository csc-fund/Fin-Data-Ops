#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_unit_test.py
# @Time      :2022/9/9 14:47
# @Author    :Colin
# @Note      :None

from table_map import *
import pandas as pd

map_app = MapCsc('CSC_Test')
tables = map_app.get_map_tables()
# 下载多源数据
df1 = pd.DataFrame(
    {'date': [20210101, 20210102, 20210103], 'code': ['000001.SZ', '000001.SZ', '000001.SZ'], 'attr1': [1, 1, 1]})
df2 = pd.DataFrame(
    {'date': [20210101, 20210102, 20210103], 'code': ['000001.SZ', '000001.SZ', '000001.SZ'], 'attr1': [2, 1, 1]})
df3 = pd.DataFrame(
    {'date': [20210101, 20210102, 20220101], 'code': ['000001.SZ', '000001.SZ', '000001.SZ'], 'attr1': [2, 1, 1]})

# 更新
map_app.update_multi_data({'df1': {'table_df': df1, 'date': df1['date'], 'code': df1['code'], }})
map_app.update_multi_data({'df2': {'table_df': df2, 'date': df2['date'], 'code': df2['code'], }})
map_app.update_multi_data({'df3': {'table_df': df3, 'date': df3['date'], 'code': df3['code'], }})

# 合并
print(map_app.merge_multi_data())
# print(map_app.MULTI_DF_DICT)
