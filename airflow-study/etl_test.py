import datetime
import pendulum
import os
import json
from concurrent.futures import *
import pendulum
import pandas as pd
import numpy as np
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator  # 数据库操作
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import logging


# [START DAG] 实例化一个DAG
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
# 在DAG中定义任务
def taskflow_api_etl():
    @task  # 提取-> 从数据库按照日期提取需要的表
    def extract_sql(table_name, conn_id='airflow_db') -> pd.DataFrame:
        # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
        sql_hook = MySqlHook(mysql_conn_id=conn_id)
        # ms_hook = MsSqlHook(mssql_conn_id='')
        # -----------------执行sql查询----------------- #
        query_sql = 'SELECT * FROM `{table_name}`'.format(table_name=table_name)
        df_extract = sql_hook.get_pandas_df(query_sql)  # 从Airflow的接口返回df格式
        return df_extract  # 传递到下一个子任务

    @task  # 变换 -> 对DataFrame进行格式转换
    def transform_df(df_extract: pd.DataFrame):
        df_transform = df_extract.fillna(0)  # 数据转换
        return df_transform  # 传递到下一个子任务

    @task  # 加载-> 下载feather格式到文件系统
    def load_feather(df_transform: pd.DataFrame):
        df_transform.to_feather('load.f')  # 储存文件
        df_transform.to_csv('load.csv')  # 储存文件

    @task  # 通信-> 异常告警
    def send_msg(sub: str = '无主题', content: str = '无内容'):
        print('异常告警', sub, content)
        # return True

    # [START main_flow]  API 2.0 会根据任务流调用自动生成依赖项,不需要定义依赖
    # 配置本地日志信息
    logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                        filename='new.log',
                        filemode='a',  # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志, a是追加模式，默认如果不写的话，就是追加模式
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'  # 日志格式
                        )

    # 多进程异步执行
    append_tables = ['ab_user', 'ab_user', 'ab_user', 'ab_user']  # 需要下载的表名
    with ThreadPoolExecutor(max_workers=3) as executor:
        tasks = {executor.submit(load_feather, transform_df(extract_sql(table))): table for table in
                 append_tables}  # 返回每个线程的执行结果,submit()中填写函数和形参,让所有进程异步执行任务
        for future in as_completed(tasks):  # 完成select_table以后,遍历每个进程的执行情况
            table = tasks[future]  # 每个进程对应的的表名
            # 获取进程执行的结果
            try:
                res = future.result()
            # 结果异常处理
            except Exception as exc:
                logging.error('%s generated an exc: %s' % (table, exc))
                send_msg(table, str(exc))
            # 结果正常处理
            else:
                logging.info('%s table res %s ' % (table, res))
                send_msg(table, str(res))
    # [END main_flow]


# [END DAG]


# [START dag_invocation]
dag = taskflow_api_etl()
# [END dag_invocation]


# 调试命令
# airflow scheduler
# airflow tasks list taskflow_api_etl  --tree
# airflow tasks test taskflow_api_etl extract 20220906
# airflow webserver --port 8080
