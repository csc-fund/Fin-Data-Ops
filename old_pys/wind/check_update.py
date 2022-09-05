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
from datetime import datetime
today = datetime.now().strftime("%Y%m%d")

db = Dbutil('WindProd')

if __name__ == "__main__":
    sqls = {
    'AShareBalanceSheet': '''
    SELECT *
    FROM [wande].[dbo].[ASHAREBALANCESHEET]
    where ANN_DT = 20220226
    ''',
    'AShareCashFlow': '''
    SELECT *
    FROM [wande].[dbo].[ASHARECASHFLOW]
    where ANN_DT = 20220226
    ''',
    'AShareFinancialIndicator': '''
    SELECT *
    FROM [wande].[dbo].[ASHAREFINANCIALINDICATOR]
    where ANN_DT = 20220226
    ''',
    'AShareIncome': '''
    SELECT *
    FROM [wande].[dbo].[ASHAREINCOME]
    where ANN_DT = 20220226
    ''',
    'AShareEODDerivativeIndicator': '''
    SELECT *
    FROM [wande].[dbo].[ASHAREEODDERIVATIVEINDICATOR]
    where TRADE_DT = 20220226
    ''',
    'AShareTTMHis': '''
    SELECT *
    FROM [wande].[dbo].[ASHARETTMHIS]
    where ANN_DT = 20220226
    '''}

    db = Dbutil('WindProd')
    for k, sql in sqls.items():
        df = db.get_df_data(sql)
        df.to_csv(f"/mnt/z/temp/checkdata_20220226/{k}_{today}.txt")
    pass