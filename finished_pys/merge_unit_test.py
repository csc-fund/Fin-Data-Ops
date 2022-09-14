#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_unit_test.py
# @Time      :2022/9/9 14:47
# @Author    :Colin
# @Note      :None

from map_table import MapCsc
import pandas as pd
import numpy as np

map_app = MapCsc('CSC_Test')
tables = map_app.get_map_tables()
# 下载多源数据
df1 = pd.DataFrame(
    {'date': [20210101, 20210102, 20210103], 'code': ['000001.SZ', '000001.SZ', '000001.SZ'], 'attr1': [1, 1, 1]})
df2 = pd.DataFrame(
    {'date': [20210101, 20210102, 20210103], 'code': ['000001.SZ', '000001.SZ', '000001.SZ'], 'attr1': [2, 1, 1]})
df3 = pd.DataFrame(
    {'date': [20210101, 20210102, 20220101], 'code': ['000001.SZ', '000001.SZ', '000001.SZ'], 'attr1': [2, 1, 1]})
df4 = pd.DataFrame(
    {'date': [20210101, 20210102, 20220101], 'code': ['000005.SZ', '000001.SZ', '000001.SZ'], 'attr1': [2, 1, 1]})

# 更新
map_app.update_multi_data(
    {'df1': {'table_df': df1, 'table_date': 'date', 'table_code': 'code', 'table_name': 'df1', 'table_db': 'wind'}})
map_app.update_multi_data(
    {'df2': {'table_df': df2, 'table_date': 'date', 'table_name': 'df2', 'table_code': 'code', 'table_db': 'wind'}})
map_app.update_multi_data(
    {'df3': {'table_df': df3, 'table_date': 'date', 'table_name': 'df3', 'table_code': 'code', 'table_db': 'wind'}})
map_app.update_multi_data(
    {'df4': {'table_df': df4, 'table_date': 'date', 'table_name': 'df4', 'table_code': 'code', 'table_db': 'wind'}})

# 合并
# print(map_app.MULTI_DF_DICT['df1']['table_df'].columns)
map_app.merge_multi_data()
np_m = map_app.MULTI_DATE_CODE.iloc[:, 2:].to_numpy('<f4')
np_m = np.array(np_m)
np_m = np.where(np.isnan(np_m), 0, np_m)

# print(np_m[:, :])

# np_1 = np_m.take(1, 1)
# np_2 = np_m.take(2, 1)
# np_3 = np_m.take(3, 1)

# print(np_1 == np_3)

# 均值法

# 先排好位置,然后再比较
# np_a = np.average([np_1, np_2, np_3], axis=0)
# print(np_a.reshape(-1, 1))
# print(np.isclose(np_1, np_2, 1.e-1, 1.e-1))
# 先求出均值,再求均值距离
np_w = pd.read_feather('20220830.f')
print(np_w.columns)
