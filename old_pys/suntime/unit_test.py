from utils.db_util import DbUtil
from db_ops.configs import TXT_ROOT
from pyarrow import dataset, Table
import pandas as pd
from pathlib import Path

# tbname = ["RPT_RATING_ADJUST", "RPT_TARGET_PRICE_ADJUST", "RPT_EARNINGS_ADJUST"]

tbname = [
    # 'DER_FORECAST_RANK', # CON_DATE
    # 'DER_EXCESS_STOCK',  # declare_date OR appraisal_date OR ALL
    # 'DER_PROB_EXCESS_STOCK', # declare_date OR ALL
    # 'DER_CONF_STK', # CON_DATE
    # 'DER_FOCUS_STK', # CON_DATE
    # 'DER_DIVER_STK',  # CON_DATE
    # 'DER_CON_DEV_STK', # CON_DATE
    # 'DER_CON_DEV_ROLL_STK', # CON_DATE
    # 'BAS_STK_HISDISTRIBUTION',  # ENTRYTIME
    # 'DER_FORECAST_ADJUST_NUM', # CON_DATE
    # 'DER_RATING_STRENGTH_RANK',  # CON_DATE
    # 'DER_FORECAST_ROLL_RANK', # CON_DATE
    # "FIN_PERFORMANCE_FORECAST"
    "BAS_STK_HISDISTRIBUTION"
]

sis = [20070101, 20080101, 20090101, 20100101, 20110101, 20120101,
       20130101, 20140101, 20150101, 20160101, 20170101, 20180101,
       20190101, 20200101, 20210101, 20220101]
eis = [20071231, 20081231, 20091231, 20101231, 20111231, 20121231,
       20131231, 20141231, 20151231, 20161231, 20171231, 20181231,
       20191231, 20201231, 20211231, 20220720]

for tb in tbname:
    for si, ei in zip(sis, eis):

        test_sql = f"SELECT * FROM {tb} WHERE trunc(ENTRYTIME) BETWEEN to_date('{si}','yyyy/MM/dd') AND to_date('{ei}','yyyy/MM/dd')"   # BAS_COMP_CAPSTRUCTURE   CON_FORECAST_CICC
        #  to_date('%s','yyyy-MM-dd')
        # test_sql = f"SELECT * FROM {tb} WHERE CON_DATE BETWEEN to_date('{si}','yyyy-MM-dd') AND to_date('{ei}','yyyy-MM-dd')"
       
        db0 = DbUtil("ZYYX")
        df =db0.get_df_data(test_sql)
        # pdf = Table.from_pandas(df)
        # dataset.write_dataset(pdf, f"/mnt/t/{tb}.f", format="feather")
        for i in df.columns:
                print(f",[{i}]")
        for i in df.columns:
            print(f"\"{i}\": \"{i.lower()}\",")
        pass

        df.fillna(value=pd.np.nan, inplace=True)

        obj_cols = df.select_dtypes(include="O").columns
        df.loc[:, obj_cols] = df.loc[:, obj_cols].astype(str)

        mainpath = Path(TXT_ROOT) / tb
        mainpath.mkdir(exist_ok=True)

        df.reset_index(drop=True).to_feather(mainpath / f"{si}_{ei}.f")
    pass

    # dtype = df.dtypes
    # df = df.astype(dtype)
    # df.CONTENT = df.CONTENT.astype(str)
    # # df.drop("CONTENT",inplace=True, axis=1)

    # df.to_parquet(f"/mnt/t/{tb}.f", engine="fastparquet", compression="gzip")