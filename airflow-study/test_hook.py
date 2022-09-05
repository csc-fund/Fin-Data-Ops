import datetime
import pendulum
import os
import json

import pendulum

from airflow.decorators import dag, task
import requests
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


# 初始化一个DAG
@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
# 定义ETL任务,包含了3个子任务
def ETL():
    # 定义子任务:提取
    # 用保存的动态连接进行数据库操作
    create_local_table = MySqlOperator(
        task_id="create_local_table",  # 任务名
        mysql_conn_id="create_local_table",  # 连接名
        sql="""
                CREATE TABLE IF NOT EXISTS employees (
                    "Serial Number" NUMERIC PRIMARY KEY,
                    "Company Name" TEXT,
                    "Employee Markme" TEXT,
                    "Description" TEXT,
                    "Leave" INTEGER
                );""",
    )

    # 定义子任务:提取
    @task()
    def extract():
        conn = MySqlHook(mysql_conn_id='airflow_db').get_conn()
        cur = conn.cursor()
