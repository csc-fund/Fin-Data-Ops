#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :test_avg.py
# @Time      :2022/9/14 09:18
# @Author    :Colin
# @Note      :None


from map_table import MapCsc
import pandas as pd
import numpy as np

from pandas.testing import assert_frame_equal

array1 = np.array([['000001.SZ', 20220101, 1, 0, 0, 0],
                   ['000001.SZ', 20220102, 1, 0, 0, 1],
                   ['000001.SZ', 20220103, 1, 0, 0, 1],
                   ['000001.SZ', 20220104, 1, 0, 0, 1],
                   ['000001.SZ', 20220105, 1, 0, 0, 1],
                   ])

array2 = np.array([['000001.SZ', 20220101, 1, 0, 0, 9],
                   ['000001.SZ', 20220102, 1, 0, 0, 1],
                   ['000001.SZ', 20220103, 1, 0, 0, 1],
                   ['000001.SZ', 20220105, 1, 0, 0, 1],
                   ['000001.SZ', 20220106, 1, 0, 0, 1],
                   ])

code_date_column = ['csc_code', 'csc_date']
attr_column = ['csc_attr1', 'csc_attr2', 'csc_attr3', 'csc_attr4']
df1 = pd.DataFrame(array1, columns=['code', 'date', 'attr1', 'attr2', 'attr3', 'attr4']).set_index(['code', 'date'])
df2 = pd.DataFrame(array2, columns=['code2', 'date2', 'attr1', 'attr2', 'attr3', 'attr4']).set_index(['code2', 'date2'])

all_date_code = pd.DataFrame(columns=code_date_column).set_index(code_date_column)
# print(df_date_code.columns)
all_date_code.index = all_date_code.index.append([df1.index, df2.index]).drop_duplicates()
df1 = all_date_code.join(df1, on=all_date_code.index.names, ).astype('float')
df2 = all_date_code.join(df2, on=all_date_code.index.names, ).astype('float')

df_3 = df1.compare(df2, keep_equal=True)

# 选出相同的.astype('float')
#
# np1 = df1.to_numpy('<f4')
# np2 = df2.to_numpy('<f4')
# assert_frame_equal()
# # 求均值
np_avg = np.average([df1, df2], 0)
# # 所有的数据不能偏离平均值差异,选出存在差异的元素
same_mask = (np.abs(np_avg - df1) <= 0.1) * (np.abs(np_avg - df2) <= 0.1)
# ---------------相同的部分---------------#
# same_data = np.where(same_mask, df1, np.nan)  # 用任意一个数据源填充相同的部分
df_merge = pd.DataFrame(same_mask * df1).replace('', np.nan).set_axis(attr_column, axis=1)
# same_date_code = all_date_code.loc[all_date_code.index[same_mask.all(1)], :] #选出相同代码和日期的行
# df_same = same_date_code.join(df1, on=same_date_code.index.names)  # 用任意一个数据源填充相同的部分

# ---------------差异的部分---------------#
df1_diff = df1.loc[~same_mask.all(1), ~same_mask.all(0)]
df2_diff = df2.loc[~same_mask.all(1), ~same_mask.all(0)]
# diff_date_code = all_date_code.loc[all_date_code.index[~same_mask.all(1)], :]
# diff_date_code = diff_date_code.loc[:, all_date_code.index[~same_mask.all(0)]]
# df_diff1 = diff_date_code.join(df1.loc[:, df1.columns[~same_mask.all(0)]], on=diff_date_code.index.names, )
# df_diff2 = diff_date_code.join(df2.loc[:, df2.columns[~same_mask.all(0)]], on=diff_date_code.index.names, )

# ---------------处理的部分---------------#


# # 输出差异数据
# # 1.先选择行 2.再选择列
# diff_data = pd.DataFrame(np1[~same_mask.all(1), :],
#                            columns=np.array(attr_column)[~same_mask.all(0)],)

# diff_data2 = np2[~same_mask.all(1), :][:, ~same_mask.all(0)]
# diff_con = np.concatenate([diff_data, diff_data2], axis=1)
