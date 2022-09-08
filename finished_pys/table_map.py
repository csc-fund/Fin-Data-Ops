#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None

MAP_DICT = {'CSC_Balance_Sheet': {'wind': {'AShareBalanceSheet': {'target_column': ['*'], 'date_column': 'ann_date', }},
                                  'suntime': {
                                      'fin_balance_sheet_gen': {'target_column': ['*'], 'date_column': 'ann_date', }}},

            'CSC_CashFlow': {'wind': {'AShareCashFlow': {'target_column': ['*'], 'date_column': 'ann_date', }},
                             'suntime': {'fin_cash_flow_gen': {'target_column': ['*'], 'date_column': 'ann_date', }}},

            'CSC_Income': {'wind': {'AShareIncome': {'target_column': ['*'], 'date_column': 'ann_date', }},
                           'suntime': {'fin_income_gen': {'target_column': ['*'], 'date_column': 'ann_date', }}},

            'CSC_Prices': {'wind': {'AShareEODPrices': {'target_column': ['*'], 'date_column': 'ann_date', }},
                           'suntime': {'qt_stk_daily': {'target_column': ['*'], 'date_column': 'ann_date', }}},

            'CSC_Derivative': {
                'wind': {'AShareEODDerivativeIndicator': {'target_column': ['s_val_mv'], 'date_column': 'ann_date', }},
                'suntime': {'qt_stk_daily': {'target_column': ['tcap'], 'date_column': 'ann_date', }}},

            'CSC_Dividend': {'wind': {'AShareDividend': {'target_column': ['*'], 'date_column': 'ann_date', },
                                      'AShareEXRightDividendRecord': {'target_column': ['*'],
                                                                      'date_column': 'ann_date', }},
                             'suntime': {
                                 'bas_stk_hisdistribution': {'target_column': ['*'], 'date_column': 'ann_date', }}},

            'CSC_Profit_Notice': {'wind': {'AShareProfitNotice': {'target_column': ['*'], 'date_column': 'ann_date', }},
                                  'suntime': {'fin_performance_forecast': {'target_column': ['*'],
                                                                           'date_column': 'ann_date', }}},

            'CSC_Profit_Express': {
                'wind': {'AShareProfitExpress': {'target_column': ['*'], 'date_column': 'ann_date', }},
                'suntime': {'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date', }}},

            'CSC_Test': {'wind': {'AShareProfitExpress': {'target_column': ['21', '21'], 'date_column': 'ann_date', },
                                  'AShareProfitExpressb': {'target_column': ['*'], 'date_column': 'ann_date', }},
                         'suntime': {'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date', }}},
            }


class MapCsc:
    def __init__(self):
        self.MAP_DICT = MAP_DICT
        self.air_flow_connector = '_af_connector'  # 数据库连接器的名称,数据源+后缀命名

    def get_map_tables(self, csc_merge_table) -> list:
        """
        :param 传入1个目标合并表
        :return:返回该目标合并表映射的的多数据源所有表
        """
        all_map_tables = []
        for db in [i for i in self.MAP_DICT[csc_merge_table].keys()]:
            all_map_tables += [(db + self.air_flow_connector, table,
                                ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr['target_column']))),
                                attr['date_column'],) for table, attr in self.MAP_DICT[csc_merge_table][db].items()]
        return all_map_tables

    def get_csc_tables(self):
        return self.MAP_DICT.keys()

# demo
# app = MapCsc()
# for i in app.get_csc_tables():
#     print(i, app.get_map_tables(i))
# print(MapCsc().get_map_tables('CSC_Profit_Notice'))
