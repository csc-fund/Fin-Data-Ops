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


# 初始化一个DAG
@dag(
    schedule_interval=timedelta(seconds=5),
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
        return df_extract

    @task  # 定义子任务:变换
    def transform(df_extract: pd.DataFrame):
        df_transform = df_extract.fillna(0).replace(np.nan, None)  # 数据转换
        return df_transform

    @task  # 定义子任务:加载
    def load(df_transform: pd.DataFrame):
        df_transform.to_csv('test.csv')

    # 添加任务之间的依赖关系
    extract() >> transform() >> load()


dag = taskflow_api_etl()
