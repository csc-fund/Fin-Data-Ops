import cx_Oracle as cxo
import pandas as pd
import numpy as np
from .db_config import DB_CONFIG
import os

# os.environ["path"] =r"E:/software/instantclient_19_3;"+os.environ["path"]
# os.environ["path"] = "E:/softwarae/instantclient_19_3"
# print(os.environ['path'])
class DbUtil:
    def __init__(self, schema):
        self.configs = DB_CONFIG[schema]
        self.db_con = self.__connect()
        self.db_con.current_schema = "ZYYX"
        
    def __connect(self):
        dsn = cxo.makedsn(host=self.configs["IP"], port=self.configs["PORT"], service_name=self.configs["SERVICE_NAME"])
        return cxo.connect(user=self.configs["USER_NAME"], password=self.configs["PASSWORD"], dsn=dsn, encoding="UTF-8",
                           nencoding="UTF-8")

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


if __name__ == "__main__":
    test_sql = "SELECT * FROM BAS_COMP_CAPSTRUCTURE "   # BAS_COMP_CAPSTRUCTURE   CON_FORECAST_CICC
    db0 = DbUtil("ZYYX")
    print(db0.get_df_data(test_sql))
