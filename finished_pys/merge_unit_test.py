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
df4 = pd.DataFrame(
    {'date': [20210101, 20210102, 20220101], 'code': ['000005.SZ', '000001.SZ', '000001.SZ'], 'attr1': [2, 1, 1]})

# df_mer3 = df3[['date', 'code3']].rename(columns={'date': 'csc_date', 'code3': 'csc_code'})
# df_mer4 = df4[['date', 'code4']].rename(columns={'date': 'csc_date', 'code4': 'csc_code'})
df_date_code = [
    i['table_df'].loc[:, [i['table_date'], i['table_code']]].rename(columns={'date': 'csc_date', 'code': 'csc_code'})
    for i in {'df3': {'table_df': df3, 'table_date': 'date', 'table_code': 'code'},
              'df4': {'table_df': df4, 'table_date': 'date', 'table_code': 'code'}}.values()
    ]
MULTI_DATE_CODE = pd.concat([i for i in df_date_code], axis=0).drop_duplicates()
print(MULTI_DATE_CODE)
# df_con = pd.concat([df_mer3, df_mer4], axis=0)
# print(df_con)
# df_con = pd.concat([df3, df4], keys=[], axis=0)
# print(df_con)
# 更新
# map_app.update_multi_data({'df1': {'table_df': df1, 'table_date': 'date', 'table_code': 'code', }})
# map_app.update_multi_data({'df2': {'table_df': df2, 'table_date': 'date', 'table_code': 'code', }})
# map_app.update_multi_data({'df3': {'table_df': df3, 'table_date': 'date', 'table_code': 'code', }})
# map_app.update_multi_data({'df4': {'table_df': df4, 'table_date': 'date', 'table_code': 'code', }})

# 合并
# print(map_app.merge_multi_data())
# print(map_app.MULTI_DF_DICT)
