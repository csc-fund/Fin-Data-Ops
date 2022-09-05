# !/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: wp
# Date: 20180803

import sys
import pyodbc
import pymssql
import pandas as pd
import numpy as np
from dbutils.pooled_db import PooledDB
from .db_config import DB_CONFIG

# from utils.db_config import DB_CONFIG
wrap = "select * FROM openquery(WANDE,'{sql}')"


class Dbutil:
    def __init__(self, db_name):
        self.db_name = db_name
        self.db_con = self.__connect(db_name)
        # DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.5.so.2.1}

    def __connect(self, db_name):
        db_desc = DB_CONFIG[db_name]
        if self.db_name == "WindProd_2":
            dbconn = pymssql.connect(host='109.244.74.214', port=4108, user='fund_wcc', password='fund_wcc',
                                     charset="CP936", tds_version="8.0")
        else:
            # conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;Trusted connection=YES;UID=%s; PWD=%s;charset=utf8;PORT=%s' % (
            #     db_desc["dbaddress"], db_desc["bookdsn"], db_desc["user"], db_desc["pwd"], db_desc.get("port", 1433))
            # dbconn = pyodbc.connect(conn_str, autocommit=False)
            self.pool = PooledDB(
                creator=pymssql,
                host = db_desc["dbaddress"],
                port = db_desc["port"],
                user = db_desc["user"],
                password = db_desc["pwd"],
                database = db_desc["bookdsn"],
                charset="utf8",    
                maxconnections=10,
                maxcached=10,  #最大空闲数
                blocking=True
            )
            dbconn = self.pool.connection()
        return dbconn

    def close(self):
        self.db_con.close()

    def get_raw_data(self, sql):
        cursor = self.db_con.cursor()
        cursor.execute(sql)
        row = cursor.fetchall()
        return row

    def get_df_data(self, sql):
        if self.db_name == "WindProd_2":
            sql = wrap.format(sql=sql)
            df = pd.read_sql(sql, self.db_con)
        else:
            df = pd.read_sql(sql, self.db_con)
        return df

    def get_df_with_type(self, sql):
        try:
            cursor = self.db_con.cursor()
            cursor.execute(sql)
            des = np.array(cursor.description)
            cols = des[:, 0]
            dtypes = des[:, 1]
            rows = cursor.fetchall()
            df = pd.DataFrame(np.array(rows), columns=cols)
            return df, cols, dtypes
        except:
            return None, None, None

    def __enter__(self): #上下文管理器
        self.db_con = self.__connect(self.db_name)
        return self
    
    def __exit__(self, type, value ,trace):
        self.db_con.close()

