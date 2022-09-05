import json
from .logger import logger, LOGGER_ROOT
import pandas as pd
import numpy as np
import os
import datetime
import sys
from pathlib import Path
pth = str(Path(__file__).parent)
print(pth)
sys.path.append(pth)


from utils.db_util import DbUtil


from .configs import TXT_ROOT, JSON_PATH


class Worker:
    def __init__(self, table_name, txt_root, table_json):
        self.table_name = table_name
        self.TXT_ROOT = txt_root
        self.folder = os.path.join(self.TXT_ROOT, self.table_name)
        self.sql, self.full = self.__parse_json(table_json)     # 解析json，drop_col没有用处 TODO
        self.logger = logger
        try:
            self.con = DbUtil("ZYYX")
        except:
            logger.error("[databse connect 2] [suntime] failed at [%s]" % (datetime.now(), ))
            try: 
                self.con = DbUtil("ZYYX_2")
                logger.error("[databse connect 2] [suntime] failed at [%s]" % (datetime.now(), ))
            except:
                raise
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

    def update(self, start_dt, end_dt, mode, fd):
        '''
        :param start_dt:  开始日期 eg. 20070101
        :param end_dt:  结束日期 eg. 20170101
        :param mode: 'append' or 'overwrite' or None == overwrite
        :param fd: 最后n天强制覆盖
        :return:
        '''
        self.mode = mode
        pth = ""
        pd_dates = pd.date_range(str(start_dt), str(end_dt))
        dates = pd_dates[:-1]
        self.log_time("[start]")
        if self.full:
            files = os.listdir(self.folder)
            if files is not None:
                for file in files:
                    try:
                        file_pth = os.path.join(self.folder, file)
                        self.rec_size(file_pth, end_dt, "old")
                        os.remove(file_pth)
                    except Exception:
                        self.logger.error(
                            "[removal failed] [%s]" % (self.table_name))

            str_t = dates[-1].strftime("%Y%m%d")
            sql = self.sql % self.table_name
            pth = os.path.join(self.folder, str_t+".f")
            if self.mode == 'append' and os.path.exists(pth):
                self.logger.info("[exists] [%s] [%s]" %
                                 (self.table_name, str_t))
                return
            self.update_one_date(sql, str_t, pth, end_dt)
            self.check(pth)
            self.logger.info("[success] [%s] [%s]" % (self.table_name, str_t))
        else:
            for t in dates:
                str_t = t.strftime("%Y%m%d")
                sql = self.sql % (self.table_name, t.strftime("%Y-%m-%d"))
                pth = os.path.join(self.folder, str_t+".f")

                if self.mode == 'append' and os.path.exists(pth):
                    if t in dates[:-fd]:
                        continue
                    self.logger.info("[force overwrite][%s] [%s]" %
                                     (self.table_name, str_t))
                self.update_one_date(sql, str_t, pth, end_dt)
                self.logger.info("[success] [%s] [%s]" %
                                 (self.table_name, str_t))
        self.check(pth)
        self.log_time("[finished]")

    def update_one_date(self, sql, date, pth, end_dt):
        try:
            df = self.con.get_df_data(sql)
        except Exception as e:
            self.logger.error("[sql error][%s]" % e)
            raise e
        # replace None with nan df.to_feather(pth, sep = '|', index = False)
        if os.path.exists(pth):
            self.rec_size(pth, end_dt, "old")
        df.fillna(value=pd.np.nan, inplace=True)
        if df.empty:
            self.logger.warn("[file empty] [%s] [%s]" % (self.table_name, date))

        obj_cols = df.select_dtypes(include="O").columns
        df.loc[:, obj_cols] = df.loc[:, obj_cols].astype(str)

        df.reset_index(drop=True).to_feather(pth)
            
        self.rec_size(pth, end_dt, "new")

    def __parse_json(self, table_json):
        with open(table_json, "r", encoding="utf-8") as f:
            json_data = json.loads(f.read())["tables"]
        try:
            sql = json_data[self.table_name]["sql"]
        except KeyError:
            self.logger.error("[wrong table name] [update] [%s] failed at [%s]" % (
                self.table_name, datetime.datetime.now()))
            raise
        is_full = json_data[self.table_name]["full"]
        if len(sql) == 0:
            if is_full:
                sql = "SELECT * FROM %s"  # TODO
            else:
                sql = "SELECT * FROM %s WHERE trade_date = %s"
        return sql, is_full

    def log_time(self, msg):
        self.logger.info("[%s][%s] at [%s]" %
                         (msg, self.table_name, datetime.datetime.now()))

    def check(self, pth):
        files = os.listdir(self.folder)
        if self.full:
            if len(files) != 1:
                self.logger.warning(
                    "[check file number failed] [%s]" % self.table_name)
        if not os.path.exists(pth):
            self.logger.warning(
                "[check file existence failed] [%s]" % self.table_name)

    def rec_size(self, pth, end_dt, rec_type):
        with open(os.path.join(LOGGER_ROOT, f"{end_dt}_size_rec.log"), "a") as f:
            print(rec_type, pth, os.path.getsize(pth), file=f, sep=",")


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Update ZYYX")
    parser.add_argument("-t", action="store", dest="table_name")
    parser.add_argument("-st", action="store", dest="start_date", type=int)
    parser.add_argument("-ed", action="store", dest="end_date", type=int)
    parser.add_argument('-md', action="store", dest="update_mode")
    parser.add_argument('--otp', action="store", dest="output_path")
    parser.add_argument('--fu', action="store", dest="force_update", type=int)
    parserGroup = parser.parse_args()

    if parserGroup.end_date is None:
        parserGroup.end_date = int(datetime.date.today().strftime('%Y%m%d'))

    if parserGroup.output_path is not None:
        TXT_ROOT = parserGroup.output_path

    if parserGroup.table_name is None or parserGroup.update_mode is None:
        logger.error("[need args]")
    else:
        w = Worker(parserGroup.table_name, TXT_ROOT, JSON_PATH)
        w.update(parserGroup.start_date, parserGroup.end_date,
                 parserGroup.update_mode, parserGroup.force_update)

    # w = Worker("DER_FORECAST_RANK", TXT_ROOT, JSON_PATH)
    # w.update(20070101, 20211110, "overwrite", 3)
