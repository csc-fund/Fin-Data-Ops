#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :test_avg.py
# @Time      :2022/9/14 09:18
# @Author    :Colin
# @Note      :None


from map_table import MapCsc
import pandas as pd
import numpy as np

array1 = np.array([['000001.SZ', 20220101, 1, 0, 0, 0],
                   ['000001.SZ', 20220101, 1, 0, 0, 1],
                   ['000001.SZ', 20220101, 1, 0, 0, 1],
                   ['000001.SZ', 20220101, 1, 0, 0, 1],
                   ['000001.SZ', 20220101, 1, 0, 0, 1],
                   ])

array2 = np.array([[1, 0, 1, 0, 0, 8],
                   [0, 1, 1, 0, 0, 2],
                   [0, 0, 1, 0, 1, 1],
                   [0, 1, 1, 0, 0, 1],
                   [0, 0, 1, 0, 0, 1],
                   ])

df1 = pd.DataFrame(array1, columns=['code', 'date', 'attr1', 'attr2', 'attr3', 'attr4']).set_index(['code', 'date'])
df2 = pd.DataFrame(array2, columns=['code', 'date', 'attr1', 'attr2', 'attr3', 'attr4']).set_index(['code', 'date'])
#
# print(pd.merge(df1, df1, 'left', ['code', 'date']))
#
# # 求均值
# np_avg = np.average([array1, array2], 0)
# # 所有的数据不能偏离平均值差异,选出存在差异的元素
# diff_mask = (np.abs(np_avg - array1) >= 0.1) * (np.abs(np_avg - array2) >= 0.1)
# # 填充
# same_data = np.where(~diff_mask, array1, np.nan)  # 用任意一个数据源填充相同的部分
#
# print(same_data)
#
# # 输出差异数据
# # 1.先选择行 2.再选择列
# # print(diff_mask.any(0))
# diff_data = array1[diff_mask.any(1), :][:, diff_mask.any(0)]
# diff_data2 = array2[diff_mask.any(1), :][:, diff_mask.any(0)]
# diff_con = np.concatenate([diff_data, diff_data2], axis=1)
# print(diff_con)
