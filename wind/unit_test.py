'''
        SELECT c.name as tablename, b.name as colname, a.name as type
        FROM sys.types a join sys.columns b on a.system_type_id = b.system_type_id
        inner join sys.tables c on b.object_id = c.object_id
'''
from utils.db_util import Dbutil
import sys
import os 
from db_ops.worker import Worker
from db_ops.configs import TXT_ROOT
from db_ops.logger import logger
import sys
import pyodbc
import pymssql
import pandas as pd
import numpy as np
from dbutils.pooled_db import PooledDB
from utils.db_config import DB_CONFIG

db = Dbutil('WindProd')

if __name__ == "__main__":
    # path = os.path.dirname(__file__)
    # sys.path.append(path)
    # import datetime
    # import argparse

    # parser = argparse.ArgumentParser(description='Update WindDB')
    # parser.add_argument("-t", action="store", dest="table_name", default="AShareEODPrices")
    # parser.add_argument('-st', action="store", dest="start_date", type=int, default=20191012)
    # parser.add_argument('--ed', action="store", dest="end_date", type=int, default=int(datetime.date.today().strftime('%Y%m%d')))
    # parser.add_argument('-md', action="store", dest="update_mode", default="append")
    # parser.add_argument('--otp', action="store", dest="output_path", default="/mnt/z/qtDOperation/windtxt2/")
    # parser.add_argument('--fu', action="store", dest="force_update", type=int, default=3)
    # parserGroup = parser.parse_args()

    # if parserGroup.end_date is None:
    #     parserGroup.end_date = int(datetime.date.today().strftime('%Y%m%d'))

    # if parserGroup.output_path is not None:
    #     TXT_ROOT = parserGroup.output_path

    # if parserGroup.table_name is None or parserGroup.update_mode is None:
    #     logger.error("[need args]")
    # else:
    #     w = Worker(parserGroup.table_name, TXT_ROOT)
    #     w.update(parserGroup.start_date, parserGroup.end_date,
    #         parserGroup.update_mode, parserGroup.force_update)

    class TestDB:

        def __init__(self, db_name):
            self.db_name = db_name
            self.db_con = self.__connect(db_name)


        def __connect(self, db_name):
            """Set up the DB-API 2 connection pool.
            creator: either an arbitrary function returning new DB-API 2
                connection objects or a DB-API 2 compliant database module
            mincached: initial number of idle connections in the pool
                (0 means no connections are made at startup)
            maxcached: maximum number of idle connections in the pool
                (0 or None means unlimited pool size)
            maxshared: maximum number of shared connections
                (0 or None means all connections are dedicated)
                When this maximum number is reached, connections are
                shared if they have been requested as shareable.
            maxconnections: maximum number of connections generally allowed
                (0 or None means an arbitrary number of connections)
            blocking: determines behavior when exceeding the maximum
                (if this is set to true, block and wait until the number of
                connections decreases, otherwise an error will be reported)
            maxusage: maximum number of reuses of a single connection
                (0 or None means unlimited reuse)
                When this maximum usage number of the connection is reached,
                the connection is automatically reset (closed and reopened).
            setsession: optional list of SQL commands that may serve to prepare
                the session, e.g. ["set datestyle to ...", "set time zone ..."]
            reset: how connections should be reset when returned to the pool
                (False or None to rollback transcations started with begin(),
                True to always issue a rollback for safety's sake)
            failures: an optional exception class or a tuple of exception classes
                for which the connection failover mechanism shall be applied,
                if the default (OperationalError, InterfaceError, InternalError)
                is not adequate for the used database module
            ping: determines when the connection should be checked with ping()
                (0 = None = never, 1 = default = whenever fetched from the pool,
                2 = when a cursor is created, 4 = when a query is executed,
                7 = always, and all other bit combinations of these values)
            args, kwargs: the parameters that shall be passed to the creator
                function or the connection constructor of the DB-API 2 module
            """
            db_desc = DB_CONFIG[db_name]
            self.pool = PooledDB(
                creator=pymssql,  # pyodbc
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

        def __enter__(self):
            self.db_con = self.__connect(self.db_name)
            return self
        
        def __exit__(self, type, value ,trace):
            self.db_con.close()


    sql = '''
        SELECT *
        FROM [wande].[dbo].[ASHARECONSENSUSROLLINGDATA]
        where EST_DT = '20220620'
        '''
    
    with TestDB('WindProd') as db:
        df = db.get_df_data(sql)
    print(df.columns)
    for i in df.columns:
        print(f",[{i}]")
    for i in df.columns:
        print(f"\"{i}\": \"{i.lower()}\",")

# convert(varchar(100), OPDATE, 112) = '%s'
    pass