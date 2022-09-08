#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None

map_dict = {'CSC_Balance_Sheet': {'wind': {'AShareBalanceSheet': ['*']}, 'suntime': {'fin_balance_sheet_gen': ['*']}},
            'CSC_CashFlow': {'wind': {'AShareCashFlow': ['*']}, 'suntime': {'fin_cash_flow_gen': ['*']}},
            'CSC_Income': {'wind': {'AShareIncome': ['*']}, 'suntime': {'fin_income_gen': ['*']}},
            'CSC_Prices': {'wind': {'AShareEODPrices': ['*']}, 'suntime': {'qt_stk_daily': ['*']}},
            'CSC_Derivative': {'wind': {'AShareEODDerivativeIndicator': ['s_val_mv']},
                               'suntime': {'qt_stk_daily': ['tcap']}},
            'CSC_Dividend': {'wind': {'AShareDividend': ['*'], 'AShareEXRightDividendRecord': ['*']},
                             'suntime': {'bas_stk_hisdistribution': ['*']}},
            'CSC_Profit_Notice': {'wind': {'AShareProfitNotice': ['*']},
                                  'suntime': {'fin_performance_forecast': ['*']}},
            'CSC_Profit_Express': {'wind': {'AShareProfitExpress': ['*']},
                                   'suntime': {'fin_performance_express': ['*']}},
            'CSC_Test': {'wind': {'AShareProfitExpress': ['fin_performance_express', 'fin_performance_express',
                                                          'fin_performance_express'],'AShareProfitExpressb':'*'},
                         'suntime': {'fin_performance_express': ['*']}},
            }
