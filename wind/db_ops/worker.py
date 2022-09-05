from .sql import sql_sentence
from pathlib import Path
import sys

pth = str(Path(__file__).parent)
print(pth)
sys.path.append(pth)
from wind.utils.db_util import *
from .table_attr import is_full
from .logger import logger, LOGGER_ROOT
import pandas as pd
import os
from datetime import datetime
import numpy as np


class Worker:
    def __init__(self, table_name, txt_root):
        self.TXT_ROOT = txt_root
        self.table_name = table_name
        self.folder = os.path.join(self.TXT_ROOT, self.table_name)
        self.logger = logger
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

    def update(self, start_dt, end_dt, mode, fd):
        self.mode = mode
        pth = ""
        pd_dates = pd.date_range(str(start_dt), str(end_dt))
        dates = [x.strftime("%Y%m%d") for x in pd_dates][:-1]
        try:
            self.con = Dbutil("WindProd")
        except:
            logger.error("[databse connect 1] [wind] failed at [%s]" % (datetime.now(),))
            try:
                self.con = Dbutil("WindProd_2")
            except:
                logger.error("[databse connect 2] [wind] failed at [%s]" % (datetime.now(),))
                raise
        try:
            self.sql = sql_sentence[self.table_name]
        except KeyError as e:
            self.logger.error("[wrong table name] [update] [%s] failed at [%s]" % (self.table_name, datetime.now()))
            raise e
        if is_full[self.table_name]:  # 清理全量表所在目录下的txt文件
            files = os.listdir(self.folder)
            if files is not None:
                for file in files:
                    try:
                        file_pth = os.path.join(self.folder, file)
                        self.rec_size(file_pth, end_dt, "old")
                        os.remove(file_pth)
                    except Exception as e:
                        self.logger.error("[removal failed] [%s]" % (self.table_name))
                        raise e
            str_t = dates[-1]  # 更新到最近一天前
            sql = self.sql
            pth = os.path.join(self.folder, str_t + ".f")
            if self.mode == 'append' and os.path.exists(pth):
                self.logger.info("[exists] [%s] [%s]" % (self.table_name, str_t))
                return
            self.update_one_date(sql, pth, str_t, end_dt)
            self.check(pth)
            self.logger.info("[success] [%s] [%s]" % (self.table_name, str_t))
        else:
            for str_t in dates:  ## 遍历更新增量表
                sql = self.sql % str_t
                pth = os.path.join(self.folder, str_t + ".f")

                if self.mode == 'append' and os.path.exists(pth):
                    if str_t in dates[:-fd]:
                        continue
                    self.logger.info("[force overwrite][%s] [%s]" % (self.table_name, str_t))
                self.update_one_date(sql, pth, str_t, end_dt)
                self.logger.info("[success] [%s] [%s]" % (self.table_name, str_t))
            self.check(pth)
        self.con.close()

    def update_one_date(self, sql, pth, date, end_dt):
        try:
            df = self.con.get_df_data(sql)
        except Exception as e:
            self.logger.error("[sql error][%s]" % e)
            raise ValueError("[sql error][%s]" % e)
        if os.path.exists(pth):
            self.rec_size(pth, end_dt, "old")
        df.fillna(value=np.nan, inplace=True)  # replace None with nan df.to_feather(pth, sep = '|', index = False)
        if df.empty:
            self.logger.warn("[file empty] [%s] [%s]" % (self.table_name, date))
        obj_cols = df.select_dtypes(include="O").columns
        df.loc[:, obj_cols] = df.loc[:, obj_cols].astype(str)
        df.reset_index(drop=True).to_feather(pth)
        self.rec_size(pth, end_dt, "new")

    def check(self, pth):
        files = os.listdir(self.folder)
        if is_full[self.table_name]:
            if len(files) != 1:
                self.logger.warning("[check file number failed] [%s]" % self.table_name)
        if not os.path.exists(pth):
            self.logger.warning("[check file existence failed] [%s]" % self.table_name)

    def rec_size(self, pth, end_dt, rec_type):
        with open(os.path.join(LOGGER_ROOT, f"{end_dt}_size_rec.log"), "a") as f:
            print(rec_type, pth, os.path.getsize(pth), file=f, sep=",")
