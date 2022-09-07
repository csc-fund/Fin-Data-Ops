import pendulum
from airflow.decorators import dag, task
# -----------------airflow数据库接口----------------- #
from airflow.providers.mysql.hooks.mysql import MySqlHook  # MySql数据库
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  # MsSql数据库
from concurrent.futures import *  # 多进程并行
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# 配置本地日志信息
logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename='new.log',
                    filemode='a',  # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志, a是追加模式，默认如果不写的话，就是追加模式
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'  # 日志格式
                    )


# 定义失败提醒,任意一个任务失败整个DAG失败
def task_failure_alert(context):
    from msg.MailMsg import MailMsg
    with MailMsg() as msg:
        msg.send_msg('任务失败',
                     f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


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
def taskflow_api_etl():
    @task(on_failure_callback=task_failure_alert)  # 提取-> 从数据库按照日期提取需要的表
    def extract_sql(table_name, conn_id='airflow_db') -> pd.DataFrame:
        # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
        sql_hook = MySqlHook(mysql_conn_id=conn_id)
        # ms_hook = MsSqlHook(mssql_conn_id='')
        # -----------------执行sql查询----------------- #
        query_sql = 'SELECT * FROM `{table_name}`'.format(table_name=table_name)
        df_extract = sql_hook.get_pandas_df(query_sql)  # 从Airflow的接口返回df格式
        return df_extract  # 传递到下一个子任务

    @task(on_failure_callback=task_failure_alert)  # 变换 -> 对DataFrame进行格式转换
    def transform_df(df_extract: pd.DataFrame):
        df_transform = df_extract.fillna(0)  # 数据转换
        return df_transform  # 传递到下一个子任务

    @task(on_success_callback=dag_success_alert)  # 加载-> 下载feather格式到文件系统
    def load_feather(df_transform: pd.DataFrame):
        df_transform.to_csv('load.csv')  # 储存文件

    @task  # 通信-> 异常告警
    def send_msg(sub: str = '无主题', content: str = '无内容'):
        from msg.MailMsg import MailMsg
        with MailMsg() as msg:
            msg.send_msg(sub, content)

    # [START main_flow]  API 2.0 会根据任务流调用自动生成依赖项,不需要定义依赖
    append_tables = ['ab_user', 'error_test', 'error_test']  # 待完成的任务:表名
    with ThreadPoolExecutor(max_workers=3) as executor:  # 多进程异步执行
        _ = {executor.submit(lambda x: load_feather(transform_df(extract_sql(x))),
                             table): table for table in append_tables}  # 每个线程的执行结果,submit()中填写函数和形参

    # [END main_flow]


# [END DAG]


# [START dag_invocation]
dag = taskflow_api_etl()
# on_success_callback
# [END dag_invocation]


# 调试命令
# airflow scheduler
# airflow tasks list taskflow_api_etl  --tree
# airflow tasks test taskflow_api_etl extract 20220906
# airflow webserver --port 8080
