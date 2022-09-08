#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None

import pendulum
from airflow.decorators import dag, task
# -----------------airflow数据库接口----------------- #
from airflow.providers.mysql.hooks.mysql import MySqlHook  # MySql数据库
from airflow.providers.common.sql.hooks.sql import DbApiHook  # MySql数据库
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  # MsSql数据库
from concurrent.futures import *  # 多进程并行
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from table_map import map_dict

# 配置本地日志信息
logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename='new.log',
                    filemode='a',  # 模式，有w和a，w就是写模式，a是追加模式，
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'  # 日志格式
                    )


# 定义失败提醒,任意一个任务失败整个DAG失败
def task_failure_alert(context):
    from msg.MailMsg import MailMsg
    with MailMsg() as msg:
        msg.send_msg('任务失败',
                     f"任务失败了!, 失败任务ID是: {context['task_instance_key_str']}")


# 定义成功提醒 放在任务流的末尾,所有任务流成功即DAG成功
def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")


# [START DAG] 实例化一个DAG
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
# 在DAG中定义任务
def csc_database_etl():
    @task(on_failure_callback=task_failure_alert)  # 提取-> 从数据库按照日期提取需要的表
    def extract_sql(connector_id, table_name, column, start_date, end_date):
        # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
        sql_hook = DbApiHook(default_conn_name=connector_id)
        # -----------------执行sql查询----------------- #
        query_sql = f'SELECT {column} FROM `{table_name}` WHERE `ann_date` BETWEEN {start_date} AND {end_date} '
        df_extract = sql_hook.get_pandas_df(query_sql)  # 从Airflow的接口返回df格式
        return df_extract, table_name  # 传递到下一个子任务

    @task(on_failure_callback=task_failure_alert)  # 变换 -> 对DataFrame进行格式转换
    def transform_df(df_extract: pd.DataFrame, table_name):
        df_transform = df_extract.fillna(0)  # 数据转换
        return df_transform, table_name  # 传递到下一个子任务

    @task(on_success_callback=dag_success_alert, on_failure_callback=task_failure_alert)  # 加载-> 下载feather格式到文件系统
    def load_feather(df_transform: pd.DataFrame, table_name):
        df_transform.to_csv(f'{table_name}.csv')  # 储存文件

    @task  # 通信-> 异常告警
    def send_msg(sub: str = '无主题', content: str = '无内容'):
        from msg.MailMsg import MailMsg
        with MailMsg() as msg:
            msg.send_msg(sub, content)

    # [START main_flow]  API 2.0 会根据任务流调用自动生成依赖项,不需要定义依赖

    # 根据映射关系获取待下载的所有数据源
    def down_by_merge(csc_merge_table: list):
        # 1.根据映射关系获取待下载的所有数据源 map_dict[csc_merge_table].keys()

        # 2.获取每个数据源下的所有表
        all_tables = []
        for db in [i for i in map_dict[csc_merge_table].keys()]:
            all_tables += [
                (db + '_af_connector', table, ','.join(list(map(lambda x: '`' + x + '`' if x != '*' else x, attr))),
                 ) for table, attr in map_dict[csc_merge_table][db].items()]
        return all_tables

    # 按照规则运行所有任务流
    def start_tasks(csc_merge_table):
        # 执行SQL的SELECT
        for i in down_by_merge(csc_merge_table):
            load_feather(transform_df(extract_sql(i[0], i[1], i[2], 20220101, 20220401)))

    with ThreadPoolExecutor(max_workers=3) as executor:  # 多进程异步执行
        _ = {executor.submit(start_tasks, table): table for table in map_dict.keys()}  # 每个线程的执行结果,submit()中填写函数和形参

    # [END main_flow]


# [END DAG]


# [START dag_invocation]
dag = csc_database_etl()
# on_success_callback
# [END dag_invocation]


# 调试命令
# airflow scheduler
# airflow tasks list taskflow_api_etl  --tree
# airflow tasks test taskflow_api_etl extract 20220906
# airflow webserver --port 8080
