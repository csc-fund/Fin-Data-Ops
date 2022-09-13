#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None


# AirFlow连接器的名称
import time

import pandas as pd

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
}


# 多源数据处理的类
class MapCsc:
    def __init__(self, csc_merge_table):
        # --------------配置文件-------------- #
        self.AF_CONN = AF_CONN  # 数据库连接器的名称,数据源+后缀命名
        self.MAP_DICT = MAP_DICT  # 静态的字典文件
        # --------------要处理的数据-------------- #
        self.CSC_MERGE_TABLE = csc_merge_table  # 输入的CSC表名
        self.MULTI_MAP_TABLES = self.get_map_tables()  # 返回的CSC对应的所有表
        self.MULTI_DF_DICT = {}  # 多数据源的df字典
        self.MULTI_DATE_CODE = pd.DataFrame()  # 多源数据公共的日期与股票代码

    # 返回映射的表
    def get_map_tables(self) -> list:
        """
        :return:[( connector_id, table_name, column, date_column,code_column ),...]
        """
        all_map_tables = []
        for db in [i for i in self.MAP_DICT[self.CSC_MERGE_TABLE].keys()]:
            all_map_tables += [(db + self.AF_CONN, table,
                                ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr['target_column']))),
                                attr['date_column'], attr['code_column'],) for table, attr in
                               self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]
        return all_map_tables

    # 从外部更新多源数据
    def update_multi_data(self, df_dict: dict):
        self.MULTI_DF_DICT.update(df_dict)

    # 合并多源数据
    def merge_multi_data(self) -> pd.DataFrame:

        # 日期和代码提取出来
        df_date_code = [pd.DataFrame([i['table_df'][i['table_code']], i['table_df'][i['table_date']]]).transpose()
                        for i in self.MULTI_DF_DICT.values()]
        # print(df_date_code[0])

        # 拼起来,并去掉code和date完全一样的行,得到面板数据的标识列
        self.MULTI_DATE_CODE = pd.concat([i for i in df_date_code], axis=0).drop_duplicates()

        # 合并所有字段
        for name, value in self.MULTI_DF_DICT.items():
            self.MULTI_DATE_CODE = pd.merge(left=self.MULTI_DATE_CODE, right=value['table_df'], how='left',
                                            left_on=['code', 'date'], right_on=['code', 'date'])

        return self.MULTI_DATE_CODE

    # 没什么用
    def get_csc_tables(self):
        return self.MAP_DICT.keys()
