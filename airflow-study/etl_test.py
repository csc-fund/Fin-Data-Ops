import datetime
import pendulum
import os
import json

import pendulum
import pandas as pd
import numpy as np
from airflow.decorators import dag, task
import requests
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta


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
    def extract() -> pd.DataFrame:
        sql_hook = MySqlHook(mysql_conn_id='airflow_db')  # 数据库连接
        # -----------------sql查询----------------- #
        query_sql = 'SELECT * FROM `{table_name}`'.format(table_name='ab_user')
        df_extract = sql_hook.get_pandas_df(query_sql)  # 直接从Airflow的接口返回df格式
        return df_extract  # 传递到下一个子任务

    @task  # 定义子任务:变换 -> 对DataFrame进行格式转换
    def transform(df_extract: pd.DataFrame):
        df_transform = df_extract.fillna(0)  # 数据转换
        return df_transform  # 传递到下一个子任务

    @task  # 定义子任务: 加载-> 下载feather格式到文件系统
    def load(df_transform: pd.DataFrame):
        df_transform.to_feather('load.f')  # 储存文件

    # [START main_flow]  API2.0本身会根据任务流自动生成依赖项,因此不需要定义依赖
    task1 = extract()
    task2 = transform(task1)
    load(task2)
    # task1 >> task2
    # [END main_flow]


# [START dag_invocation]
dag = taskflow_api_etl()
# [END dag_invocation]
