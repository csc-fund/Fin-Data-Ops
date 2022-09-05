#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :download_table.py
# @Time      :2022/9/5 13:16
# @Author    :Colin
# @Note      :None
# airflow db init
#
# airflow users create \
#     --username admin \
#     --firstname Peter \
#     --lastname Parker \
#     --role Admin \
#     --email spiderman@superhero.org
#
# airflow webserver --port 8080
#
# airflow scheduler

#

# initialize the database tables
# airflow db init
#
# # print the list of active DAGs
# airflow dags list
#
# # prints the list of tasks in the "tutorial" DAG
# airflow tasks list tutorial
#
# # prints the hierarchy of tasks in the "tutorial" DAG
# airflow tasks list download_table  --tree
# The DAG object; we'll need this to instantiate a DAG

# airflow tasks list download_table  --tree
# airflow tasks test download_table select

from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

import pendulum
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
import mysql.connector
from datetime import datetime, timedelta

def select_mysql(*args, **context):
    # 使用airflow 的 Connections 动态获取配置信息
    from airflow.hooks.base import BaseHook
    from airflow.hooks import mysql_hook #
    from airflow.providers.mysql.operators.mysql import MySqlOperator
    conn = BaseHook.get_connection('airflow_db')
    # create=MySqlOperator.execute()
    mydb = mysql.connector.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        port=conn.port
    )

    mycursor = mydb.cursor()

    sql = """INSERT INTO stock_prices_stage
            (ticker, as_of_date, open_price, high_price, low_price, close_price)
            VALUES (%s,%s,%s,%s,%s,%s)"""
    mycursor.execute(sql, ('0', '0', '0', '0', '0', '0'))

    mydb.commit()
    mydb.close()


# from airflow.providers.sqlite
# [START instantiate_dag]
with DAG(
        'download_table',
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={'retries': 2},
        # [END default_args]
        description='ETL DAG tutorial',
        schedule_interval=timedelta(seconds=5),
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['example'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__

    # [START select_task]
    select_task = PythonOperator(
        task_id="select",
        python_callable=select_mysql,
        provide_context=True
    )
