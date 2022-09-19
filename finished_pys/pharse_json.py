#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :pharse_json.py
# @Time      :2022/9/15 11:32
# @Author    :Colin
# @Note      :None
import json
import collections


def update_map_dict() -> dict:
    """
    从映射表获取映射关系,并用数据字典里面返回对应的字段更新映射字典
    :return:
    """
    map_dict_path = 'map_dict.json'  # 字典文件所在的目录
    with open(map_dict_path) as f:
        MAP_DICT = json.load(f)  # 静态的字典文件

    def get_same_column(merge_table):
        """
        从数据字典json文件获取相同中文字段
        :param merge_table:
        """
        # 取出所有的数据库和表名
        multi_dbs = list(MAP_DICT[merge_table].keys())
        multi_tables = [list(i.keys()) for i in list(MAP_DICT[merge_table].values())]
        # 相同字段字典
        all_db_column = []
        same_ch_column = []
        for db in list(MAP_DICT[merge_table].keys()):  # 遍历数据源
            column_dicts = list(map(lambda x: get_table_columns(x), MAP_DICT[merge_table][db]))  # 得到字段字典
            all_db_column += [column_dicts]
            same_ch_column += set([j for i in column_dicts for j in list(i.keys())])  # 合并同一数据源字段+去重
        same_ch_column = [k for k, v in collections.Counter(same_ch_column).items() if v > 1]

        # 删除非共有的
        _ = [all_db_column[i][j].pop(k) for i, db_list in enumerate(all_db_column) for j, column_dict in
             enumerate(db_list) for k in list(column_dict.keys()) if k not in same_ch_column]

        # 映射回去
        _ = [MAP_DICT[merge_table][multi_dbs[i]][multi_tables[i][m]].update({'target_column': list(n.values())})
             for i, j in enumerate(all_db_column) for m, n in enumerate(j)]

    for csc_table in MAP_DICT.keys():
        get_same_column(csc_table)

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


with open('map_tables.json', 'w') as json_file:
    json_file.write(json.dumps(update_map_dict()))
