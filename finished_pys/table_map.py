#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None

MAP_DICT = {'CSC_Balance_Sheet': {

    'wind': {'AShareBalanceSheet': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
    'suntime': {
        'fin_balance_sheet_gen': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_CashFlow': {
        'wind': {'AShareCashFlow': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
        'suntime': {'fin_cash_flow_gen': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Income': {'wind': {'AShareIncome': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
                   'suntime': {
                       'fin_income_gen': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Prices': {
        'wind': {'AShareEODPrices': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
        'suntime': {'qt_stk_daily': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Derivative': {
        'wind': {'AShareEODDerivativeIndicator': {'target_column': ['s_val_mv'], 'date_column': 'ann_date',
                                                  'code_column': 'code'}},
        'suntime': {'qt_stk_daily': {'target_column': ['tcap'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Dividend': {
        'wind': {'AShareDividend': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'},
                 'AShareEXRightDividendRecord': {'target_column': ['*'],
                                                 'date_column': 'ann_date', 'code_column': 'code'}},
        'suntime': {
            'bas_stk_hisdistribution': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Profit_Notice': {
        'wind': {'AShareProfitNotice': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
        'suntime': {'fin_performance_forecast': {'target_column': ['*'],
                                                 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Profit_Express': {
        'wind': {'AShareProfitExpress': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
        'suntime': {
            'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}}},

    'CSC_Test': {'wind': {
        'AShareProfitExpress': {'target_column': ['21', '21'], 'date_column': 'ann_date', 'code_column': 'code'},
        'AShareProfitExpressb': {'target_column': ['*'], 'date_column': 'ann_date', 'code_column': 'code'}},
        'suntime': {'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date',
                                                'code_column': 'code'}}},
}


# 映射处理的类
class MapCsc:
    def __init__(self, csc_merge_table):
        self.AF_CONN = '_af_connector'  # 数据库连接器的名称,数据源+后缀命名
        self.MAP_DICT = MAP_DICT  # 映射字典
        self.CSC_MERGE_TABLE = csc_merge_table  # 输入的CSC表名
        self.MERGE_DF_DICT = {}  # 待合并的所有数据源

    def get_map_tables(self) -> list:
        """
        :param 传入1个目标合并表
        :return:[( connector_id, table_name, column, date_column,code_column ),...]
        """
        all_map_tables = []
        for db in [i for i in self.MAP_DICT[self.CSC_MERGE_TABLE].keys()]:
            all_map_tables += [(db + self.AF_CONN, table,
                                ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr['target_column']))),
                                attr['date_column'], attr['code_column'],) for table, attr in
                               self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]
        return all_map_tables

    def update_multi_data(self, df_dict: dict):
        self.MERGE_DF_DICT.update(df_dict)

    def get_csc_tables(self):
        return self.MAP_DICT.keys()
