import datetime
import pendulum
import os
import json
import concurrent.futures
import pendulum
import pandas as pd
import numpy as np
from airflow.decorators import dag, task
import requests
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator  # 数据库操作
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta


# mssql,ture
# airflow tasks test taskflow_api_etl extract 20220906
# 初始化一个DAG
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
# 定义ETL任务,包含了3个子任务
def taskflow_api_etl():
    @task  # 定义子任务:提取-> 从数据库按照日期提取需要的表
    def extract(table_name, conn_id='airflow_db') -> pd.DataFrame:
        # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
        sql_hook = MySqlHook(mysql_conn_id=conn_id)
        # ms_hook = MsSqlHook(mssql_conn_id='')
        # -----------------在线程池中异步执行sql查询----------------- #

        query_sql = 'SELECT * FROM `{table_name}`'.format(table_name=table_name)
        df_extract = sql_hook.get_pandas_df(query_sql)  # 直接从Airflow的接口返回df格式
        return df_extract  # 传递到下一个子任务

    @task  # 定义子任务:变换 -> 对DataFrame进行格式转换
    def transform(df_extract: pd.DataFrame):
        df_transform = df_extract.fillna(0)  # 数据转换
        return df_transform  # 传递到下一个子任务

    @task  # 定义子任务: 加载-> 下载feather格式到文件系统
    def load(df_transform: pd.DataFrame):
        df_transform.to_feather('load.f')  # 储存文件
        return True  # 返回Ture状态

    # [START main_flow]  API2.0本身会根据任务流自动生成依赖项,因此不需要定义依赖
    # 需要下载的表名
    append_tables = ['ab_user', 'ab_user_role', 'dag']

    # etl过程
    def task_flow(table_name):
        load(transform(extract(table_name)))

    # 多线程执行etl
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        tasks = {executor.submit(task_flow, table, 60): table for table in append_tables}  # 返回每个线程的执行结果
        for future in concurrent.futures.as_completed(tasks):  # 完成select_table以后
            table = tasks[future]  # 执行的表名
            try:
                res = future.result()  # 线程执行的结果
            # 每个线程的异常处理
            except Exception as exc:
                # 异常执行
                print('%r generated an exception: %s' % (table, exc))
            # 线程正常执行完成
            else:
                print('%r page is %r bytes' % (table, res))

    # [END main_flow]


# [START dag_invocation]
dag = taskflow_api_etl()
# [END dag_invocation]
