#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None

map_dict = {'CSC_Balance_Sheet': {'wind': {'AShareBalanceSheet': {'target_column': ['*'], 'date_column': 'ann_date', }},
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

            'CSC_Test': {'wind': {'AShareProfitExpress': {'target_column': ['21','21'], 'date_column': 'ann_date', },
                                  'AShareProfitExpressb': {'target_column': ['*'], 'date_column': 'ann_date', }},
                         'suntime': {'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date', }}},
            }
