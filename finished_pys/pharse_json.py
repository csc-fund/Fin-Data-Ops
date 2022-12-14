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

        # -------------- 遍历数据源-------------- #
        for db in MAP_DICT[merge_table].keys():
            column_dicts = list(map(lambda x: get_table_columns(x), MAP_DICT[merge_table][db]))  # 得到字段字典
            all_db_column += [column_dicts]
            same_ch_column += list({}.fromkeys([j for i in column_dicts for j in i.keys()]).keys())  # 合并同一数据源字段
            # 这里有提取出的所有字段,在这里观察
            print(merge_table, '\n', column_dicts)
        # -------------- 求重复字段-------------- #
        column_filter = same_ch_column
        if merge_table in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income', ]:
            pass
        elif merge_table in ['CSC_Prices']:
            column_filter = [i.split('(')[0] for i in same_ch_column]  # 过滤掉括号
            column_filter = [i.replace('复权因子', '后复权系数') for i in column_filter]
        elif merge_table in ['CSC_Derivative']:  # CSC_Profit_Express
            column_filter = [i.replace('当日总市值', '总市值') for i in column_filter]

        elif merge_table in ['CSC_Profit_Notice']:  # CSC_Profit_Express
            column_filter = [i.split('(')[0] for i in same_ch_column]  # 过滤掉括号
            column_filter = [i.replace('业绩预告类型代码', '公告类型') for i in column_filter]
            column_filter = [i.replace('预告', '') for i in column_filter]
            column_filter = [i.replace('变动幅度', '增长幅度') for i in column_filter]

        elif merge_table in ['CSC_Profit_Express']:  # CSC_Profit_Express
            column_filter = [i.split('(')[0] for i in same_ch_column]  # 过滤掉括号
            column_filter = [i.replace('归属于母公司股东的', '') for i in column_filter]
            column_filter = [i.replace('基本每股收益', '每股收益') for i in column_filter]
            column_filter = [i.split('增长率:')[1] + '同比' if '增长率:' in i else i for i in column_filter]  # 过滤掉括号

        elif merge_table in ['CSC_Profit_Express']:
            pass
        # 原有的和处理的一起排序
        sort_index = sorted(enumerate(column_filter), key=lambda x: x[1])  # 提取出排序后的序号
        column_filter = [column_filter[i[0]] for i in sort_index]
        same_ch_column = [same_ch_column[i[0]] for i in sort_index]
        # 统计字段出现次数
        count_dict = collections.Counter(column_filter)
        # 取出重复值的下标
        same_ch_column = [same_ch_column[i] for i, v in enumerate(column_filter) if count_dict[v] > 1]
        # 再次去重
        same_ch_column = list(dict.fromkeys(same_ch_column))

        # 保留共有的
        if same_chn_flag:
            for i, db_list in enumerate(all_db_column):
                for j, column_dict in enumerate(db_list):
                    # --------------自定义的全局相同列-------------- #
                    code_column = {k: all_db_column[i][j][k] for k in all_db_column[i][j].keys() if
                                   k in ['股票代码', 'Wind代码']}
                    # --------------自定义日期列-------------- #
                    date = []
                    if merge_table == 'CSC_Dividend':
                        pass
                    elif merge_table in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income', ]:
                        date = ['公告日期', '报表发布日期', '报表类型', '报告期', '报表年度']
                    elif merge_table in ['CSC_Profit_Notice', ]:
                        date = ['公告日期', '报告期', '预测周期']
                    date_column = {same: all_db_column[i][j][same] for same in date
                                   if same in all_db_column[i][j].keys()}

                    # -------------- 在same中添加,求交集并保持顺序对应-------------- #
                    all_same_sort = {same: all_db_column[i][j][same] for same in same_ch_column
                                     if same in all_db_column[i][j].keys()}

                    # -------------- 更新 -------------- #
                    code_column.update(date_column)
                    code_column.update(all_same_sort)
                    all_db_column[i][j] = code_column

        # 映射回去
        _ = [MAP_DICT[merge_table][multi_dbs[i]][multi_tables[i][m]].update(
            {'target_chn': list(n.keys()), 'target_column': list(n.values()), 'code_column': list(n.values())[0],
             'date_column': list(n.values())[1], 'index_column': [list(n.values())[0], list(n.values())[1]]
             }) for i, j in enumerate(all_db_column) for m, n in enumerate(j)]

        # index_column的映射规则
        if merge_table in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income']:
            _ = [MAP_DICT[merge_table][multi_dbs[i]][multi_tables[i][m]].update(
                {'index_column': [list(n.values())[0], list(n.values())[2], list(n.values())[1]]
                 }) for i, j in enumerate(all_db_column) for m, n in enumerate(j)]

        # 检查所有表的字段
        check_res = [(merge_table, db, MAP_DICT[merge_table][db][table]['target_chn'])
                     for db in MAP_DICT[merge_table].keys() for table in MAP_DICT[merge_table][db]]
        print('字段检查:', len(check_res[0][2]), len(check_res[1][2]), '\n', check_res[0], '\n', check_res[1], )

    # 迭代所有的表
    # _ = [update_target_column(csc_table) for csc_table in ['CSC_Profit_Notice', ]]
    _ = [update_target_column(csc_table) for csc_table in MAP_DICT.keys()]

    # 保存
    if same_chn_flag:
        with open('map_tables_same.json', 'w') as json_file:
            json_file.write(json.dumps(MAP_DICT, ensure_ascii=False))
    else:
        with open('map_tables_all.json', 'w') as json_file:
            json_file.write(json.dumps(MAP_DICT, ensure_ascii=False))
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
update_map_dict(True)
