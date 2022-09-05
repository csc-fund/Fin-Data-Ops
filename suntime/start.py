import datetime
import sys
import os
from db_ops.worker import Worker
from db_ops.configs import TXT_ROOT, JSON_PATH
from db_ops.logger import logger


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