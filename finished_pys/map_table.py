#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None


import time

import pandas as pd
import numpy as np

# AirFlow连接器的名称
AF_CONN = '_af_connector'
# 人工定义的数据映射字典
MAP_DICT = {
    'CSC_Balance_Sheet':
        {
            'wind': {
                'AShareBalanceSheet': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}
            },
            'suntime': {
                'fin_balance_sheet_gen': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}
            }
        },

    'CSC_CashFlow':
        {
            'wind': {'AShareCashFlow': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}
                     },
            'suntime': {'fin_cash_flow_gen': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}
                        }
        },

    'CSC_Income':
        {'wind': {'AShareIncome': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
         'suntime': {
             'fin_income_gen': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}
         },

    'CSC_Prices':
        {
            'wind': {'AShareEODPrices': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
            'suntime': {'qt_stk_daily': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}
        },

    'CSC_Derivative':
        {
            'wind': {'AShareEODDerivativeIndicator': {'target_column': ['s_val_mv'], 'date_column': 'ann_date',
                                                      'code_column': 'code'}},
            'suntime': {'qt_stk_daily': {'target_column': ['tcap'], 'date_column': 'ann_date', 'code_column': 'code'}}
        },

    'CSC_Dividend':
        {
            'wind': {'AShareDividend': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'},
                     'AShareEXRightDividendRecord': {'target_column': ['*'],
                                                     'date_column': 'ann_date', 'code_column': 'code'}},
            'suntime': {
                'bas_stk_hisdistribution': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}
        },

    'CSC_Profit_Notice':
        {
            'wind': {'AShareProfitNotice': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
            'suntime': {'fin_performance_forecast': {'target_column': ['*'],
                                                     'date_column': 'ann_date', 'code_column': 'code'}}
        },

    'CSC_Profit_Express':
        {
            'wind': {'AShareProfitExpress': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
            'suntime': {
                'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}
        },

    # 用于测试的表
    'CSC_Test':
        {
            'wind':
                {'AShareProfitExpress': {'target_column': ['*'], 'date_column': 'ann_date',
                                         'code_column': 'code'},
                 'AShareProfitExpressb': {'target_column': ['*'], 'date_column': 'ann_date',
                                          'code_column': 'code'}
                 },
            'suntime':
                {'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date',
                                             'code_column': 'code'}
                 }
        },
    # 用于测试的表
    'CSC_Test2':
        {
            'wind':
                {'AShareProfitExpressb': {'target_column': ['*'], 'date_column': 'ann_date',
                                          'code_column': 'code'},
                 },
            'suntime':
                {'fin_performance_expressb': {'target_column': ['*'], 'date_column': 'ann_date',
                                              'code_column': 'code'}
                 }
        },
}


# 多源数据处理的类
class MapCsc:
    def __init__(self, csc_merge_table):
        # --------------配置文件-------------- #
        self.AF_CONN = AF_CONN  # 数据库连接器的名称,数据源+后缀命名
        self.MAP_DICT = MAP_DICT  # 静态的字典文件
        # --------------要处理的数据-------------- #
        self.CSC_MERGE_TABLE = csc_merge_table  # 输入的CSC表名
        self.DB_LIST = None
        self.MULTI_MAP_TABLES = None  # 返回的CSC对应的所有表
        self.MULTI_TABLE_DICT = self.MAP_DICT[csc_merge_table]  # 多数据源的df字典
        self.MULTI_DATE_CODE = pd.DataFrame()  # 多源数据公共的日期与股票代码
        self.CODE_DATE_COLUM = ['csc_code', 'csc_date']  # 标准化的匹配字段
        self.ATTR_COLUMN = []
        self.ALL_CODE_DATE = pd.DataFrame(columns=self.CODE_DATE_COLUM).set_index(self.CODE_DATE_COLUM)  # 用于匹配的df

    # 返回映射的表
    def get_map_tables(self) -> list:
        """
        :return:[( connector_id, table_name, column, date_column,code_column,column_len ),...]
        """
        all_map_tables = []
        for db in [i for i in self.MAP_DICT[self.CSC_MERGE_TABLE].keys()]:
            all_map_tables += [(db + self.AF_CONN, table,
                                ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr['target_column']))),
                                attr['date_column'], attr['code_column'], len(attr['target_column']))
                               for table, attr in self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]
        self.MULTI_MAP_TABLES = all_map_tables
        return self.MULTI_MAP_TABLES

    # 从外部更新多源数据
    def update_multi_data(self, db, table_name, df):
        self.MULTI_TABLE_DICT[db][table_name].update({'table_df': df})

    # 初始化MULTI_TABLE_DICT
    def init_multi_data(self, MULTI_TABLE_DICT: dict):
        self.MULTI_TABLE_DICT = MULTI_TABLE_DICT

    # 合并多源数据
    def merge_multi_data(self) -> pd.DataFrame:
        # 提取代码和日期
        df_date_code = [
            i['table_df'].loc[:, [i['table_code'], i['table_date']]].rename(
                columns={i['table_code']: 'csc_code', i['table_date']: 'csc_date'})
            for i in self.MULTI_TABLE_DICT.values()]

        # 拼起来,并去掉code和date完全一样的行,得到面板数据的标识列
        self.MULTI_DATE_CODE = pd.concat([i for i in df_date_code], axis=0).drop_duplicates()

        # 合并所有字段
        for value in self.MULTI_TABLE_DICT.values():
            pre_name = value['table_db'] + '_' + value['table_name'] + '_'  # 重命名,数据来源+字段名
            self.MULTI_DATE_CODE = pd.merge(left=self.MULTI_DATE_CODE, right=value['table_df'].rename(
                columns={i: pre_name + i for i in value['table_df'].columns}), how='left',
                                            left_on=['csc_code', 'csc_date'],
                                            right_on=[pre_name + value['table_code'],
                                                      pre_name + value['table_date']]).drop(
                columns=[pre_name + value['table_code'], pre_name + value['table_date']])

        # 保存
        return self.MULTI_DATE_CODE

    # 多源数据对比
    def merge_multi_data_v2(self) -> pd.DataFrame:
        # 1.获取所有的索引
        self.ALL_CODE_DATE.index = self.ALL_CODE_DATE.index.append(
            [value['table_df'].set_index([value['code_column'], value['date_column']]).index for table_dict in
             self.MULTI_TABLE_DICT.keys() for value in self.MULTI_TABLE_DICT[table_dict].values()]).drop_duplicates()

        # 2.迭代join
        df_joins = []  # 不同数据源
        for table_dict in self.MULTI_TABLE_DICT.keys():  # ki wind,suntime
            df_db = pd.DataFrame()  # 同一数据源
            for value in self.MULTI_TABLE_DICT[table_dict].values():
                df_table = value['table_df'].set_index([value['code_column'], value['date_column']])
                df_join = self.ALL_CODE_DATE.join(df_table, on=self.ALL_CODE_DATE.index.names)
                df_db = pd.concat([df_db, df_join], axis=1)  # 按顺序拼接同一数据源
            df_joins.append(df_db)  # 不同数据源

        # 3.对比 - 只做了2个的,如果要做2个以上的需要自己写一个对比+合并函数
        self.ATTR_COLUMN = [f'attr{i}' for i in range(len(df_joins[0].columns))]
        df_joins[0].set_axis(self.ATTR_COLUMN, axis=1, inplace=True)
        df_joins[1].set_axis(self.ATTR_COLUMN, axis=1, inplace=True)
        df_compare = df_joins[0].compare(df_joins[1], keep_shape=True, keep_equal=True)
        # print(self.MULTI_TABLE_DICT.keys())
        self.DB_LIST = list(self.MULTI_TABLE_DICT.keys())
        df_compare.columns = [f'{i[1]}_{i[0]}'.replace('self', self.DB_LIST[0]).replace('other', self.DB_LIST[1])
                              for i in df_compare.columns]
        return df_compare.sort_index().reset_index()


def demo():
    df = pd.DataFrame(np.array([['000001.SZ', 20220105, 1, ],
                                ['000001.SZ', 20220104, 1, ],
                                ['000001.SZ', 20220101, 1, ],
                                ]), columns=['code', 'ann_date', 'attr1', ])
    df2 = pd.DataFrame(np.array([['000001.SZ', 20220105, 3, 2],
                                 ['000001.SZ', 20220102, 1, 2],
                                 ['000001.SZ', 20220101, 1, 5],
                                 ]), columns=['code', 'ann_date', 'attr1', 'attr2'])
    app = MapCsc('CSC_Test')
    app.update_multi_data('wind', 'AShareProfitExpress', df)
    app.update_multi_data('wind', 'AShareProfitExpressb', df)
    app.update_multi_data('suntime', 'fin_performance_express', df2)

    print(app.merge_multi_data_v2())

# demo()
