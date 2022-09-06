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
    @task  # 定义子任务:提取
    def extract() -> pd.DataFrame:
        sql_hook = MySqlHook(mysql_conn_id='airflow_db')  # 数据库连接
        query_sql = 'select * from stock_prices_stage'  # sql查询
        df_extract = sql_hook.get_pandas_df(query_sql)  # 转为df格式返回
        # df_extract.to_csv('test1.csv')
        return df_extract  # 传递到下一个子任务

    @task  # 定义子任务:变换
    def transform(df_extract: pd.DataFrame):
        df_transform = df_extract  # 数据转换
        df_transform.to_csv('test2.csv')
        return df_transform  # 传递到下一个子任务

    @task  # 定义子任务:加载
    def load(df_transform: pd.DataFrame):
        df_transform.to_csv('test3.csv')

    # [START main_flow]
    task1 = extract()
    task2 = transform(task1)
    load(task2)
    # task1 >> task2 >> task3

    # [END main_flow]


# [START dag_invocation]
dag = taskflow_api_etl()
# [END dag_invocation]
