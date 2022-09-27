#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :pharse_json.py
# @Time      :2022/9/15 11:32
# @Author    :Colin
# @Note      :None
import json
import collections
from operator import itemgetter
import pandas as pd

with open('suntime_1' + '.json') as f:  # 去数据字典文件中寻找
    suntime_tab = json.load(f)
suntime_tab1 = suntime_tab['tables']

with open('suntime_2' + '.json') as f:  # 去数据字典文件中寻找
    suntime_tab2 = json.load(f)
suntime_tab2 = suntime_tab2['tables']

suntime_tab1.update(suntime_tab2)

# with open('wind' + '.json') as f:  # 去数据字典文件中寻找
#     wind_tab = json.load(f)

from wind import sql_sentence
wind_list=[i.split('[')[-1].split(']')[0] for i in sql_sentence.values()]
wind_list=list(set(wind_list))
# print(wind_list)
# print(wind_tab)

#
df_wind = pd.DataFrame(wind_list)
df_sun = pd.DataFrame(list(suntime_tab1.keys()))
df_wind.to_csv('df_wind.csv')
df_sun.to_csv('df_sun.csv')

# res = update_map_dict()
# print(update_map_dict())
