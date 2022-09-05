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
# The DAG object; we'll need this to instantiate a DAG

from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

import pendulum
from airflow.providers.mysql.operators.mysql import MySqlOperator
import mysql.connector

# from airflow.providers.sqlite
# [START instantiate_dag]
with DAG(
        'tutorial_etl_dag_test',
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={'retries': 2},
        # [END default_args]
        description='ETL DAG tutorial',
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['example'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__


    def save_to_mysql_stage(*args, **context):
        # 使用airflow 的 Connections 动态获取配置信息
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('airflow_db')
        mydb = mysql.connector.connect(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema,
            port=conn.port
        )

        mycursor = mydb.cursor()

        mycursor.execute('CREATE IF NOT EXISTS TABLE `stock_prices_stage`')
        sql = """INSERT INTO `stock_prices_stage`
            (ticker, as_of_date, open_price, high_price, low_price, close_price)
            VALUES (%s,%s,%s,%s,%s,%s)"""
        mycursor.executemany(sql, (1, 1, 1, 1, 1, 1))

        mydb.commit()
        print(mycursor.rowcount, "record inserted.")
