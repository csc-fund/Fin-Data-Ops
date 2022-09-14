#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :test_dict.py
# @Time      :2022/9/14 17:06
# @Author    :Colin
# @Note      :None


a = {
    'wind':
        {'AShareProfitExpress': {'target_column': ['*'], 'date_column': 'ann_date',
                                 'code_column': 'code'},
         },
    'suntime':
        {'fin_performance_express': {'target_column': ['*'], 'date_column': 'ann_date',
                                     'code_column': 'code'}
         }
}
old_db = a['wind']
old_db.update({'fin_performance_express':1})
print(old_db)
