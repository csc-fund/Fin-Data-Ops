#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :pharse_json.py
# @Time      :2022/9/15 11:32
# @Author    :Colin
# @Note      :None
import json
import collections
from operator import itemgetter


def update_map_dict(same_chn_flag=False) -> dict:
    """
    从映射表获取映射关系,并用数据字典里面返回对应的字段更新映射字典
    :return:
    """
    map_dict_path = 'map_dict.json'  # 字典文件所在的目录
    with open(map_dict_path) as f:
        MAP_DICT = json.load(f)  # 静态的字典文件

    def update_target_column(merge_table):
        """
        从数据字典json文件获取相同中文字段
        :param merge_table:
        """
        # 取出所有的数据库和表名
        multi_dbs = list(MAP_DICT[merge_table].keys())
        multi_tables = [list(i.keys()) for i in list(MAP_DICT[merge_table].values())]
        all_db_column = []  # 每个表全部字段
        same_ch_column = []  # 多数据源相同˙中文名字段

        #
        for db in MAP_DICT[merge_table].keys():  # 遍历数据源
            column_dicts = list(map(lambda x: get_table_columns(x), MAP_DICT[merge_table][db]))  # 得到字段字典
            all_db_column += [column_dicts]
            same_ch_column += list({}.fromkeys([j for i in column_dicts for j in i.keys()]).keys())  # 合并同一数据源字段
            # print(list({}.fromkeys(same_ch_column).keys()))
            # print(same_ch_column)

        # 相同字段过滤规则
        if merge_table in ['CSC_Prices', ]:
            same_ch_column = [i.split('(')[0] for i in same_ch_column]

        # 选择共同字段
        same_ch_column = [k for k, v in collections.Counter(same_ch_column).items() if v > 1]

        # 需要排序,不要用pop的风格

        # 日期,代码等索引列

        # 保留共有的,注意顺序要对应上
        if same_chn_flag:

            for i, db_list in enumerate(all_db_column):
                for j, column_dict in enumerate(db_list):
                    # 自定义的全局相同列
                    code_column = {k: all_db_column[i][j][k] for k in all_db_column[i][j].keys() if
                                   k in ['股票代码', 'Wind代码']}

                    # 自定义日期列
                    date = []
                    if merge_table == 'CSC_Dividend':
                        date = []

                    elif merge_table in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income']:
                        date = ['公告日期', '报表发布日期', '报告期', '报表年度']

                    date_column = {k: all_db_column[i][j][k] for k in all_db_column[i][j].keys() if
                                   k in date}

                    # 自定义其他列
                    other = ['']
                    if merge_table == 'CSC_Derivative':
                        other = ['当日总市值', '总市值']
                    other_column = {k: all_db_column[i][j][k] for k in all_db_column[i][j].keys() if
                                    k in other}
                    # 求交集,更新
                    if merge_table in ['CSC_Prices', ]:
                        all_same = {k: all_db_column[i][j][k] for k in all_db_column[i][j].keys() if
                                    k.split('(')[0] in same_ch_column}
                    else:
                        all_same = {k: all_db_column[i][j][k] for k in all_db_column[i][j].keys() if
                                    k in same_ch_column}

                    code_column.update(date_column)
                    code_column.update(all_same)
                    code_column.update(other_column)
                    all_db_column[i][j] = code_column
                    # print(table_list)
                    # print(all_db_column[i][j])
                    # print(itemgetter(*table_list)(all_db_column[i][j]))
                    # print([i for i in itemgetter(*table_list)(all_db_column[i][j])])
                    # all_db_column[i][j] = itemgetter(*table_list)(all_db_column[i][j])
                    # print(all_db_column[i][j].update({}))
                    # for k in list(column_dict.keys()):
                    #     if k in same_ch_column:
                    #         print(all_db_column[i][j].get([k, k]))

            # print(all_db_column)
            # _ = [all_db_column[i][j].pop(k) for i, db_list in enumerate(all_db_column) for j, column_dict in
            #      enumerate(db_list) for k in list(column_dict.keys()) if k not in same_ch_column]
            # print(all_db_column[0])

        # 映射回去
        _ = [MAP_DICT[merge_table][multi_dbs[i]][multi_tables[i][m]].update(
            {'target_chn': list(n.keys()), 'target_column': list(n.values()), 'code_column': list(n.values())[0],
             'date_column': list(n.values())[1], 'index_column': [list(n.values())[0], list(n.values())[1]]
             }) for i, j in enumerate(all_db_column) for m, n in enumerate(j)]

        # 特殊的映射规则
        if merge_table in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income']:
            _ = [MAP_DICT[merge_table][multi_dbs[i]][multi_tables[i][m]].update(
                {'index_column': [list(n.values())[0], list(n.values())[3], list(n.values())[1]]
                 }) for i, j in enumerate(all_db_column) for m, n in enumerate(j)]

    # 迭代所有的表
    # _ = [update_target_column(csc_table) for csc_table in ['CSC_Derivative', ]]
    _ = [update_target_column(csc_table) for csc_table in MAP_DICT.keys()]

    return MAP_DICT


def get_table_columns(table_name: str) -> dict:
    """
    根据表名返回所有字段的中文和英文字典
    :param table_name:
    :return: {'中文名':column_name}
    """
    with open('table_dict/' + table_name + '.json') as f:  # 去数据字典文件中寻找
        DATA_DICT = json.load(f)
    return {i['fieldChsName']: i['fieldName'] for i in DATA_DICT['fieldData']}


# res = update_map_dict()
# print(update_map_dict())
with open('map_tables.json', 'w') as json_file:
    json_file.write(json.dumps(update_map_dict(True), ensure_ascii=False))
